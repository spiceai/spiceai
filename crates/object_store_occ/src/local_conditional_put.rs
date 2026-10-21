/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Wrapper around [`LocalFileSystem`] that adds conditional put support.
//!
//! [`LocalFileSystem`] returns [`Error::NotImplemented`] for `PutMode::Update`,
//! which breaks any code relying on optimistic concurrency control (`ETag`s) for
//! file-based scheduler state.
//!
//! This wrapper intercepts `PutMode::Update` requests and implements them by
//! hashing the current bytes under a file-backed advisory lock, then writing
//! with `PutMode::Overwrite` if that hash is the expected `ETag`. The token is
//! a content hash because a local file's own `ETag` is its inode, modification
//! time, and size: a rewrite of the same size can reuse an inode inside one
//! timestamp and hand a stale writer a token that matches again.
//!
//! **Concurrency model**: each conditional write takes an advisory lock for the
//! object's directory. Two clients whose roots and keys name the same file
//! therefore share one lock, including across processes. A dedicated filesystem
//! worker retains that lock through publication after caller cancellation or
//! Tokio shutdown. Accepted writes may finish after their caller stops waiting.
//! The lock file is not an object and is not removed: unlinking it would let
//! the next writer lock a different inode.
//!
//! [`LocalFileSystem`]: object_store::local::LocalFileSystem
//! [`Error::NotImplemented`]: object_store::Error::NotImplemented

use std::fmt::{self, Display, Formatter};
use std::fs::OpenOptions;
use std::io;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

/// Wraps a [`LocalFileSystem`] to add `PutMode::Update` support.
///
/// For `PutMode::Update`, it:
/// 1. Acquires the directory advisory lock
/// 2. Reads the object and hashes its bytes
/// 3. Compares that hash with the expected `ETag` from the [`UpdateVersion`]
/// 4. Writes with `PutMode::Overwrite` if they match
/// 5. Releases the advisory lock
///
/// `ETag`s this store returns are that same hash, including from `put`, `get`,
/// `head`, and listings.
///
/// Other operations are delegated. The directory lock path is not an object.
///
/// [`UpdateVersion`]: object_store::UpdateVersion
pub struct LocalConditionalPut {
    inner: Arc<LocalFileSystem>,
    root: PathBuf,
    writes: tokio::sync::mpsc::Sender<ConditionalWrite>,
}

struct ConditionalWrite {
    location: Path,
    payload: PutPayload,
    options: PutOptions,
    reply: tokio::sync::oneshot::Sender<Result<PutResult, ObjectStoreError>>,
}

fn worker_error(source: impl std::error::Error + Send + Sync + 'static) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "LocalConditionalPut",
        source: Box::new(source),
    }
}

fn is_publication_lock(location: &Path) -> bool {
    matches!(
        location.filename(),
        Some(".spice-occ-lock" | ".spice-object-store-occ.lock")
    )
}

fn lock_file(path: &std::path::Path) -> Result<std::fs::File, ObjectStoreError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(worker_error)?;
    file.lock().map_err(worker_error)?;
    Ok(file)
}

/// Hash of the object bytes. This is the conditional-write token.
fn content_etag(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn payload_etag(payload: &PutPayload) -> String {
    let mut hasher = blake3::Hasher::new();
    for chunk in payload {
        hasher.update(chunk);
    }
    hasher.finalize().to_hex().to_string()
}

async fn stored_etag(inner: &LocalFileSystem, location: &Path) -> Result<String, ObjectStoreError> {
    let bytes = inner.get(location).await?.bytes().await?;
    Ok(content_etag(&bytes))
}

fn bytes_result(
    meta: ObjectMeta,
    attributes: Attributes,
    bytes: bytes::Bytes,
) -> Result<GetResult, ObjectStoreError> {
    let len = u64::try_from(bytes.len()).map_err(|_| ObjectStoreError::Generic {
        store: "LocalConditionalPut",
        source: "object is larger than a u64 length".into(),
    })?;
    Ok(GetResult {
        payload: GetResultPayload::Stream(
            futures::stream::once(async move { Ok::<_, ObjectStoreError>(bytes) }).boxed(),
        ),
        meta,
        range: 0..len,
        attributes,
    })
}

fn reserved_lock_error(location: &Path) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "LocalConditionalPut",
        source: format!("path {location} is reserved for the conditional-write lock").into(),
    }
}

/// One lock file per directory. Equivalent store roots then share it, and
/// deleting an object does not leave a lock behind for every key.
fn publication_lock_path(root: &std::path::Path, location: &Path) -> PathBuf {
    let object_path = root.join(location.as_ref());
    object_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(root)
        .join(".spice-occ-lock")
}

/// Runs outside any Tokio runtime so `LocalFileSystem` performs its filesystem
/// calls synchronously. The lock and publication have one owner, even when the
/// caller drops its future or shuts its runtime down. The bounded queue limits
/// pending payloads; closing all senders drains accepted writes and exits.
fn write_worker(
    inner: &LocalFileSystem,
    root: &std::path::Path,
    mut receive: tokio::sync::mpsc::Receiver<ConditionalWrite>,
) {
    while let Some(request) = receive.blocking_recv() {
        let result = conditional_write(
            inner,
            root,
            &request.location,
            request.payload,
            request.options,
        );
        // A dropped receiver means an unknown outcome for the caller, not abort.
        let _ = request.reply.send(result);
    }
}

fn conditional_write(
    inner: &LocalFileSystem,
    root: &std::path::Path,
    location: &Path,
    payload: PutPayload,
    mut options: PutOptions,
) -> Result<PutResult, ObjectStoreError> {
    let legacy_path = root.join(".spice-object-store-occ.lock");
    let directory_path = publication_lock_path(root, location);
    if let Some(parent) = directory_path.parent() {
        std::fs::create_dir_all(parent).map_err(worker_error)?;
    }
    // Take the legacy root lock before the directory lock. An older process
    // locks only the root file. Reversing this order can deadlock with a writer
    // that still uses this order.
    let _legacy_guard = lock_file(&legacy_path)?;
    let _directory_guard = lock_file(&directory_path)?;
    // No async executor task owns these guards: every filesystem operation runs
    // to completion on this thread before its file descriptor releases the lock.
    let etag = payload_etag(&payload);
    futures::executor::block_on(async {
        if let PutMode::Update(expected) = &options.mode {
            let current = inner.get(location).await?.bytes().await?;
            let found = content_etag(&current);
            // Compare the bytes, not the filesystem ETag. inode + mtime + size
            // can recur for a same-size rewrite, and a stale writer would then
            // overwrite every update that landed in between.
            if expected.e_tag.as_deref() != Some(found.as_str()) {
                return Err(ObjectStoreError::Precondition {
                    path: location.to_string(),
                    source: format!(
                        "ETag mismatch: expected {:?}, found {found}",
                        expected.e_tag
                    )
                    .into(),
                });
            }
            options.mode = PutMode::Overwrite;
        }
        let mut written = inner.put_opts(location, payload, options).await?;
        written.e_tag = Some(etag);
        Ok(written)
    })
}

impl std::fmt::Debug for LocalConditionalPut {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalConditionalPut")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl LocalConditionalPut {
    /// Creates a new `LocalConditionalPut` rooted at `root`.
    ///
    /// The directory at `root` must already exist; callers should create it
    /// beforehand (e.g. via `std::fs::create_dir_all`).
    ///
    /// # Errors
    ///
    /// Returns an error if the `LocalFileSystem` cannot be initialized
    /// (e.g. `root` does not exist or is not a directory).
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ObjectStoreError> {
        let root = root.into();
        let inner = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
        let (writes, receive) = tokio::sync::mpsc::channel(1);
        let worker_store = Arc::clone(&inner);
        let worker_root = root.clone();
        std::thread::Builder::new()
            .name("local-state-writer".to_owned())
            .spawn(move || write_worker(&worker_store, &worker_root, receive))
            .map_err(worker_error)?;
        Ok(Self {
            inner,
            root,
            writes,
        })
    }
}

impl Display for LocalConditionalPut {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "LocalConditionalPut({})", self.root.display())
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl ObjectStore for LocalConditionalPut {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        if is_publication_lock(location) {
            return Err(reserved_lock_error(location));
        }
        if matches!(opts.mode, PutMode::Update(_)) {
            let (reply, result) = tokio::sync::oneshot::channel();
            self.writes
                .send(ConditionalWrite {
                    location: location.clone(),
                    payload,
                    options: opts,
                    reply,
                })
                .await
                .map_err(|_| worker_error(io::Error::other("local state writer stopped")))?;
            result.await.map_err(worker_error)?
        } else {
            let etag = payload_etag(&payload);
            let mut written = self.inner.put_opts(location, payload, opts).await?;
            written.e_tag = Some(etag);
            Ok(written)
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        if is_publication_lock(location) {
            return Err(reserved_lock_error(location));
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        if is_publication_lock(location) {
            return Err(ObjectStoreError::NotFound {
                path: location.to_string(),
                source: "reserved for the conditional-write lock".into(),
            });
        }
        // Preconditions name the content hash this store publishes. The inner
        // store would judge them against its inode/mtime/size ETag instead.
        let mut passthrough = options.clone();
        passthrough.if_match = None;
        passthrough.if_none_match = None;
        passthrough.if_modified_since = None;
        passthrough.if_unmodified_since = None;
        let whole_body = passthrough.range.is_none() && !passthrough.head;
        let mut result = self.inner.get_opts(location, passthrough).await?;
        if whole_body {
            let attributes = result.attributes.clone();
            let mut meta = result.meta.clone();
            let bytes = result.bytes().await?;
            meta.e_tag = Some(content_etag(&bytes));
            options.check_preconditions(&meta)?;
            return bytes_result(meta, attributes, bytes);
        }
        let full = self.inner.get(location).await?.bytes().await?;
        result.meta.e_tag = Some(content_etag(&full));
        options.check_preconditions(&result.meta)?;
        Ok(result)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        let inner = Arc::clone(&self.inner);
        self.inner
            .list(prefix)
            .try_filter(|meta| futures::future::ready(!is_publication_lock(&meta.location)))
            .and_then(move |mut meta| {
                let inner = Arc::clone(&inner);
                async move {
                    meta.e_tag = Some(stored_etag(&inner, &meta.location).await?);
                    Ok(meta)
                }
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |meta| futures::future::ready(meta.location > offset))
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result
            .objects
            .retain(|meta| !is_publication_lock(&meta.location));
        for meta in &mut result.objects {
            meta.e_tag = Some(stored_etag(&self.inner, &meta.location).await?);
        }
        Ok(result)
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
        let inner = Arc::clone(&self.inner);
        locations
            .then(move |item| {
                let inner = Arc::clone(&inner);
                async move {
                    let path = item?;
                    if is_publication_lock(&path) {
                        return Err(reserved_lock_error(&path));
                    }
                    let mut deleted =
                        inner.delete_stream(futures::stream::once(async move { Ok(path) }).boxed());
                    match deleted.next().await {
                        Some(result) => result,
                        None => Err(worker_error(io::Error::other("delete produced no result"))),
                    }
                }
            })
            .boxed()
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), ObjectStoreError> {
        if is_publication_lock(from) || is_publication_lock(to) {
            return Err(reserved_lock_error(if is_publication_lock(to) {
                to
            } else {
                from
            }));
        }
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<(), ObjectStoreError> {
        if is_publication_lock(from) || is_publication_lock(to) {
            return Err(reserved_lock_error(if is_publication_lock(to) {
                to
            } else {
                from
            }));
        }
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use object_store::UpdateVersion;

    use super::*;

    #[tokio::test]
    async fn test_basic_put_and_get() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");

        let path = Path::from("test/basic.json");
        let data = b"hello world";
        let result = store
            .put_opts(
                &path,
                data.as_slice().into(),
                PutOptions::from(PutMode::Overwrite),
            )
            .await
            .expect("put should succeed");

        assert!(result.e_tag.is_some());

        let get_result = store.get(&path).await.expect("get should succeed");
        let bytes = get_result.bytes().await.expect("read bytes");
        assert_eq!(bytes.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn test_create_mode() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");

        let path = Path::from("test/create.json");

        // First create should succeed
        store
            .put_opts(
                &path,
                b"first".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("first create should succeed");

        // Second create should fail
        let result = store
            .put_opts(
                &path,
                b"second".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await;

        assert!(
            matches!(result, Err(ObjectStoreError::AlreadyExists { .. })),
            "second create should fail with AlreadyExists, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_conditional_update_success() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");

        let path = Path::from("test/update.json");

        // Create the initial object
        let create_result = store
            .put_opts(
                &path,
                b"v1".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create should succeed");

        let version = UpdateVersion {
            e_tag: create_result.e_tag.clone(),
            version: None,
        };

        // Update with correct ETag should succeed
        let update_result = store
            .put_opts(
                &path,
                b"v2".as_slice().into(),
                PutOptions::from(PutMode::Update(version)),
            )
            .await
            .expect("conditional update should succeed");

        assert!(update_result.e_tag.is_some());

        // Verify the data was updated
        let get_result = store.get(&path).await.expect("get should succeed");
        let bytes = get_result.bytes().await.expect("read bytes");
        assert_eq!(bytes.as_ref(), b"v2");
    }

    #[tokio::test]
    async fn publication_lock_is_hidden_and_cannot_be_replaced() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");
        let path = Path::from("cluster/state.json");
        let created = store
            .put_opts(
                &path,
                b"v1".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create");
        store
            .put_opts(
                &path,
                b"v2".as_slice().into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: created.e_tag,
                    version: None,
                })),
            )
            .await
            .expect("update");

        let locks = [
            (
                Path::from("cluster/.spice-occ-lock"),
                dir.path().join("cluster/.spice-occ-lock"),
            ),
            (
                Path::from(".spice-object-store-occ.lock"),
                dir.path().join(".spice-object-store-occ.lock"),
            ),
        ];
        for (_, lock_file) in &locks {
            assert!(
                lock_file.is_file(),
                "conditional update must create {}",
                lock_file.display()
            );
        }
        let listed = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        assert!(
            listed
                .iter()
                .all(|meta| !is_publication_lock(&meta.location)),
            "list must hide both locks: {listed:?}"
        );
        for (lock, lock_file) in locks {
            assert!(
                matches!(
                    store.get(&lock).await,
                    Err(ObjectStoreError::NotFound { .. })
                ),
                "get must not expose {lock}"
            );
            assert!(
                store.delete(&lock).await.is_err(),
                "delete must not unlink {lock}"
            );
            assert!(
                store
                    .put(&lock, b"replaced".as_slice().into())
                    .await
                    .is_err(),
                "put must not replace {lock}"
            );
            assert!(lock_file.is_file(), "{} must remain", lock_file.display());
            assert_eq!(std::fs::read(&lock_file).expect("read lock"), b"");
        }
    }

    #[tokio::test]
    async fn test_conditional_update_stale_etag() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");

        let path = Path::from("test/stale.json");

        // Create the initial object
        let create_result = store
            .put_opts(
                &path,
                b"v1".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create should succeed");

        let stale_version = UpdateVersion {
            e_tag: create_result.e_tag.clone(),
            version: None,
        };

        // Overwrite the object to change its ETag
        store
            .put_opts(
                &path,
                b"v2".as_slice().into(),
                PutOptions::from(PutMode::Overwrite),
            )
            .await
            .expect("overwrite should succeed");

        // Update with stale ETag should fail with Precondition
        let result = store
            .put_opts(
                &path,
                b"v3".as_slice().into(),
                PutOptions::from(PutMode::Update(stale_version)),
            )
            .await;

        assert!(
            matches!(result, Err(ObjectStoreError::Precondition { .. })),
            "stale update should fail with Precondition, got: {result:?}"
        );

        // Verify the data was NOT updated
        let get_result = store.get(&path).await.expect("get should succeed");
        let bytes = get_result.bytes().await.expect("read bytes");
        assert_eq!(bytes.as_ref(), b"v2");
    }

    #[tokio::test]
    async fn test_conditional_update_not_found() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");

        let path = Path::from("test/nonexistent.json");
        let version = UpdateVersion {
            e_tag: Some("fake-etag".to_string()),
            version: None,
        };

        let result = store
            .put_opts(
                &path,
                b"data".as_slice().into(),
                PutOptions::from(PutMode::Update(version)),
            )
            .await;

        assert!(
            matches!(result, Err(ObjectStoreError::NotFound { .. })),
            "update of nonexistent should fail with NotFound, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_sequential_conditional_updates() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");

        let path = Path::from("test/sequential.json");

        // Create
        let r1 = store
            .put_opts(
                &path,
                b"v1".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create");

        // Update 1
        let v1 = UpdateVersion {
            e_tag: r1.e_tag,
            version: None,
        };
        let r2 = store
            .put_opts(
                &path,
                b"v2".as_slice().into(),
                PutOptions::from(PutMode::Update(v1)),
            )
            .await
            .expect("update 1");

        // Update 2
        let v2 = UpdateVersion {
            e_tag: r2.e_tag,
            version: None,
        };
        let r3 = store
            .put_opts(
                &path,
                b"v3".as_slice().into(),
                PutOptions::from(PutMode::Update(v2)),
            )
            .await
            .expect("update 2");

        // Update 3
        let v3 = UpdateVersion {
            e_tag: r3.e_tag,
            version: None,
        };
        store
            .put_opts(
                &path,
                b"v4".as_slice().into(),
                PutOptions::from(PutMode::Update(v3)),
            )
            .await
            .expect("update 3");

        // Verify final value
        let bytes = store
            .get(&path)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(bytes.as_ref(), b"v4");
    }

    #[tokio::test]
    async fn test_conditional_update_compares_etag_without_version() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");

        let path = Path::from("test/etag-only.json");
        let create_result = store
            .put_opts(
                &path,
                b"v1".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create should succeed");

        let version = UpdateVersion {
            e_tag: create_result.e_tag,
            version: Some("local-store-has-no-version".to_string()),
        };

        store
            .put_opts(
                &path,
                b"v2".as_slice().into(),
                PutOptions::from(PutMode::Update(version)),
            )
            .await
            .expect("etag-matched update should ignore version");

        let bytes = store
            .get(&path)
            .await
            .expect("get should succeed")
            .bytes()
            .await
            .expect("read bytes");
        assert_eq!(bytes.as_ref(), b"v2");
    }

    #[tokio::test]
    async fn test_concurrent_conditional_updates_one_wins() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store =
            std::sync::Arc::new(LocalConditionalPut::new(dir.path()).expect("create store"));

        let path = Path::from("test/concurrent.json");

        // Create initial value
        let create_result = store
            .put_opts(
                &path,
                b"v1".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create");

        // Two "concurrent" writers both see the same ETag
        let same_version = UpdateVersion {
            e_tag: create_result.e_tag,
            version: None,
        };

        let store1 = std::sync::Arc::clone(&store);
        let v1 = same_version.clone();
        let p1 = path.clone();
        let h1 = tokio::spawn(async move {
            store1
                .put_opts(
                    &p1,
                    b"writer-1".as_slice().into(),
                    PutOptions::from(PutMode::Update(v1)),
                )
                .await
        });

        let store2 = std::sync::Arc::clone(&store);
        let v2 = same_version;
        let p2 = path.clone();
        let h2 = tokio::spawn(async move {
            store2
                .put_opts(
                    &p2,
                    b"writer-2".as_slice().into(),
                    PutOptions::from(PutMode::Update(v2)),
                )
                .await
        });

        let (r1, r2) = tokio::join!(h1, h2);
        let r1 = r1.expect("join h1");
        let r2 = r2.expect("join h2");

        // Exactly one should succeed and one should fail
        let successes = [r1.is_ok(), r2.is_ok()];
        assert_eq!(
            successes.iter().filter(|&&s| s).count(),
            1,
            "exactly one writer should succeed; r1={r1:?}, r2={r2:?}"
        );
        assert!(
            matches!(r1, Err(ObjectStoreError::Precondition { .. }))
                || matches!(r2, Err(ObjectStoreError::Precondition { .. })),
            "exactly one writer should fail with Precondition; r1={r1:?}, r2={r2:?}"
        );

        let final_bytes = store
            .get(&path)
            .await
            .expect("get final value")
            .bytes()
            .await
            .expect("read final bytes");
        assert!(
            matches!(final_bytes.as_ref(), b"writer-1" | b"writer-2"),
            "final value should be from the winning writer, got {final_bytes:?}"
        );
    }

    #[tokio::test]
    async fn test_concurrent_conditional_updates_across_instances_one_wins() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let first_store =
            std::sync::Arc::new(LocalConditionalPut::new(dir.path()).expect("create first store"));
        let second_store =
            std::sync::Arc::new(LocalConditionalPut::new(dir.path()).expect("create second store"));

        let path = Path::from("test/concurrent-across-instances.json");
        let create_result = first_store
            .put_opts(
                &path,
                b"v1".as_slice().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create");
        let same_version = UpdateVersion {
            e_tag: create_result.e_tag,
            version: None,
        };

        let store1 = std::sync::Arc::clone(&first_store);
        let v1 = same_version.clone();
        let p1 = path.clone();
        let h1 = tokio::spawn(async move {
            store1
                .put_opts(
                    &p1,
                    b"writer-1".as_slice().into(),
                    PutOptions::from(PutMode::Update(v1)),
                )
                .await
        });

        let store2 = std::sync::Arc::clone(&second_store);
        let p2 = path.clone();
        let h2 = tokio::spawn(async move {
            store2
                .put_opts(
                    &p2,
                    b"writer-2".as_slice().into(),
                    PutOptions::from(PutMode::Update(same_version)),
                )
                .await
        });

        let (r1, r2) = tokio::join!(h1, h2);
        let r1 = r1.expect("join h1");
        let r2 = r2.expect("join h2");

        assert_eq!(
            [r1.is_ok(), r2.is_ok()]
                .into_iter()
                .filter(|success| *success)
                .count(),
            1,
            "exactly one writer should succeed; r1={r1:?}, r2={r2:?}"
        );
        assert!(
            matches!(r1, Err(ObjectStoreError::Precondition { .. }))
                || matches!(r2, Err(ObjectStoreError::Precondition { .. })),
            "exactly one writer should fail with Precondition; r1={r1:?}, r2={r2:?}"
        );
    }
}
