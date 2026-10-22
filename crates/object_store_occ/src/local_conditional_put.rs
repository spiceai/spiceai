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
//! This wrapper publishes every put itself, under a file-backed advisory lock:
//! it checks the mode's precondition against the current file, writes the
//! payload to a staged file, and renames or links that file into place.
//!
//! **`ETag`s never repeat.** [`LocalFileSystem`] derives an `ETag` from the
//! file's inode, modification time in microseconds, and size. A filesystem that
//! reuses a replaced file's inode and stamps files from a coarse clock, such as
//! ext4, gives two same-size versions written within one clock tick the same
//! `ETag`, and an update built on the older version would replace the newer
//! one. So each staged version's modification time is moved past every version
//! its directory has published, recorded as the lock file's own modification
//! time, before the version is published. On a filesystem that stores coarser
//! times than a millisecond, the writer waits for the clock to pass that time
//! instead of stamping the version ahead of the clock.
//!
//! Copies, renames, and multipart uploads are delegated and publish without
//! the lock, so write a key that conditional updates guard through `put_opts`
//! alone.
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
use std::io::{self, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions,
};

/// Wraps a [`LocalFileSystem`] to add `PutMode::Update` support.
///
/// For every `put_opts`, it:
/// 1. Acquires the directory advisory lock
/// 2. Checks the mode's precondition against the current file: no file for
///    `PutMode::Create`, the expected `ETag` from the [`UpdateVersion`] for
///    `PutMode::Update`
/// 3. Writes the payload to a staged file and moves its modification time past
///    every version the directory has published
/// 4. Links the staged file into place for `PutMode::Create`, or renames it
/// 5. Releases the advisory lock
///
/// Other operations are delegated. The directory lock path is not an object.
///
/// [`UpdateVersion`]: object_store::UpdateVersion
pub struct LocalConditionalPut {
    inner: Arc<LocalFileSystem>,
    root: PathBuf,
    writes: tokio::sync::mpsc::Sender<Publication>,
}

struct Publication {
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

/// Runs on its own thread with blocking filesystem calls. The lock and
/// publication have one owner, even when the caller drops its future or shuts
/// its runtime down. The bounded queue limits pending payloads; closing all
/// senders drains accepted writes and exits.
fn write_worker(
    inner: &LocalFileSystem,
    root: &std::path::Path,
    mut receive: tokio::sync::mpsc::Receiver<Publication>,
) {
    while let Some(request) = receive.blocking_recv() {
        let result = publish(
            inner,
            root,
            &request.location,
            &request.payload,
            &request.options,
        );
        // A dropped receiver means an unknown outcome for the caller, not abort.
        let _ = request.reply.send(result);
    }
}

/// How far past the directory's last publication a staged version's
/// modification time is moved, tried in order until the filesystem stores a
/// time past it: one microsecond, the `ETag`'s resolution, then a step that
/// survives millisecond timestamp resolution.
const STAMP_STEPS: [Duration; 2] = [Duration::from_micros(1), Duration::from_millis(1)];

/// How long a filesystem that stores coarser times than a millisecond, such
/// as FAT's two seconds, may wait for the clock to pass the floor.
const COARSE_TIMESTAMP_WAIT: Duration = Duration::from_secs(5);

const COARSE_TIMESTAMP_POLL: Duration = Duration::from_millis(50);

/// A file whose modification time can be read and set: the staged file, or a
/// test double for a filesystem with coarse timestamps.
trait Timestamped {
    fn modified(&self) -> io::Result<SystemTime>;
    fn set_modified(&self, time: SystemTime) -> io::Result<()>;
}

impl Timestamped for std::fs::File {
    fn modified(&self) -> io::Result<SystemTime> {
        self.metadata()?.modified()
    }

    fn set_modified(&self, time: SystemTime) -> io::Result<()> {
        std::fs::File::set_modified(self, time)
    }
}

/// `LocalFileSystem` reads an unavailable modification time as the epoch.
fn modified(metadata: &std::fs::Metadata) -> SystemTime {
    metadata.modified().unwrap_or(UNIX_EPOCH)
}

fn micros(time: SystemTime) -> u64 {
    let micros = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    u64::try_from(micros).unwrap_or(u64::MAX)
}

#[cfg(unix)]
fn inode(metadata: &std::fs::Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(metadata)
}

#[cfg(not(unix))]
fn inode(_metadata: &std::fs::Metadata) -> u64 {
    0
}

/// The `ETag` `LocalFileSystem` reports for a file with this metadata.
/// `put_returns_the_etag_head_reports` pins the two together.
fn local_etag(metadata: &std::fs::Metadata) -> String {
    format!(
        "{:x}-{:x}-{:x}",
        inode(metadata),
        micros(modified(metadata)),
        metadata.len()
    )
}

/// Writes the payload to a new `{path}#{n}` file beside the target: the name
/// `LocalFileSystem` gives its own staged uploads and leaves out of listings.
fn stage(
    path: &std::path::Path,
    payload: &PutPayload,
) -> Result<(std::fs::File, PathBuf), ObjectStoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(worker_error)?;
    }
    let mut suffix = 1_u64;
    loop {
        let mut staging = path.as_os_str().to_owned();
        staging.push(format!("#{suffix}"));
        let staging = PathBuf::from(staging);
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&staging)
        {
            Ok(mut file) => {
                if let Err(source) = payload.iter().try_for_each(|chunk| file.write_all(chunk)) {
                    let _ = std::fs::remove_file(&staging);
                    return Err(worker_error(source));
                }
                return Ok((file, staging));
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => suffix += 1,
            Err(source) => return Err(worker_error(source)),
        }
    }
}

/// Moves the staged file's stored modification time past `floor`, at the
/// `ETag`'s microsecond resolution.
fn stamp(staged: &impl Timestamped, floor: SystemTime) -> io::Result<()> {
    let floor = micros(floor);
    let past_floor = || Ok::<_, io::Error>(micros(staged.modified()?) > floor);
    if past_floor()? {
        return Ok(());
    }
    for step in STAMP_STEPS {
        staged.set_modified(UNIX_EPOCH + Duration::from_micros(floor) + step)?;
        if past_floor()? {
            return Ok(());
        }
    }
    // No time just past the floor survives this filesystem's resolution. A
    // larger step would put the version ahead of the clock, and every write
    // faster than the resolution would push it further ahead, while heartbeat
    // and retention checks read the modification time as when it was written.
    // So wait for the clock to pass the floor. This thread is the dedicated
    // writer, not a Tokio worker.
    let deadline = std::time::Instant::now() + COARSE_TIMESTAMP_WAIT;
    loop {
        staged.set_modified(SystemTime::now())?;
        if past_floor()? {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "the filesystem kept a modification time at or before {floor} microseconds after the epoch for {COARSE_TIMESTAMP_WAIT:?}, so the new version's ETag could repeat an earlier version's"
            )));
        }
        std::thread::sleep(COARSE_TIMESTAMP_POLL);
    }
}

fn publish(
    inner: &LocalFileSystem,
    root: &std::path::Path,
    location: &Path,
    payload: &PutPayload,
    options: &PutOptions,
) -> Result<PutResult, ObjectStoreError> {
    if !options.attributes.is_empty() {
        return Err(ObjectStoreError::NotImplemented {
            operation: "`put_opts` with `opts.attributes` specified".into(),
            implementer: "LocalConditionalPut".into(),
        });
    }
    let path = inner.path_to_filesystem(location)?;
    let legacy_path = root.join(".spice-object-store-occ.lock");
    let directory_path = publication_lock_path(root, location);
    if let Some(parent) = directory_path.parent() {
        std::fs::create_dir_all(parent).map_err(worker_error)?;
    }
    // Take the legacy root lock before the directory lock. An older process
    // locks only the root file. Reversing this order can deadlock with a writer
    // that still uses this order.
    let _legacy_guard = lock_file(&legacy_path)?;
    let directory_guard = lock_file(&directory_path)?;
    let current = match std::fs::metadata(&path) {
        Ok(metadata) => Some(metadata),
        Err(source) if source.kind() == io::ErrorKind::NotFound => None,
        Err(source) => return Err(worker_error(source)),
    };
    match (&options.mode, &current) {
        (PutMode::Create, Some(_)) => {
            return Err(ObjectStoreError::AlreadyExists {
                path: location.to_string(),
                source: "an object already exists at this path".into(),
            });
        }
        (PutMode::Update(_), None) => {
            return Err(ObjectStoreError::NotFound {
                path: location.to_string(),
                source: "no object exists at this path".into(),
            });
        }
        (PutMode::Update(expected), Some(current)) => {
            // LocalFileSystem supplies ETags but no object version IDs.
            let found = local_etag(current);
            if expected.e_tag.as_deref() != Some(found.as_str()) {
                return Err(ObjectStoreError::Precondition {
                    path: location.to_string(),
                    source: format!(
                        "ETag mismatch: expected {:?}, found {:?}",
                        expected.e_tag,
                        Some(found)
                    )
                    .into(),
                });
            }
        }
        _ => {}
    }
    // The lock file's modification time is the latest one this directory has
    // published. The current file's covers a version a writer without this
    // floor published.
    let floor = current
        .as_ref()
        .map(modified)
        .into_iter()
        .chain([modified(&directory_guard.metadata().map_err(worker_error)?)])
        .max()
        .unwrap_or(UNIX_EPOCH);
    let (staged, staging) = stage(&path, payload)?;
    let published = stamp(&staged, floor).map_err(worker_error).and_then(|()| {
        let metadata = staged.metadata().map_err(worker_error)?;
        // Advance the floor before the version becomes visible, so no
        // published version is ever past it.
        directory_guard
            .set_modified(modified(&metadata))
            .map_err(worker_error)?;
        // Some FUSE filesystems, such as Blobfuse, upload a file only once
        // it is closed.
        drop(staged);
        let linked = if matches!(options.mode, PutMode::Create) {
            std::fs::hard_link(&staging, &path)
        } else {
            std::fs::rename(&staging, &path)
        };
        match linked {
            Ok(()) => Ok(metadata),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                Err(ObjectStoreError::AlreadyExists {
                    path: location.to_string(),
                    source: Box::new(source),
                })
            }
            Err(source) => Err(worker_error(source)),
        }
    });
    if published.is_err() || matches!(options.mode, PutMode::Create) {
        let _ = std::fs::remove_file(&staging);
    }
    Ok(PutResult {
        e_tag: Some(local_etag(&published?)),
        version: None,
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
        let (reply, result) = tokio::sync::oneshot::channel();
        self.writes
            .send(Publication {
                location: location.clone(),
                payload,
                options: opts,
                reply,
            })
            .await
            .map_err(|_| worker_error(io::Error::other("local state writer stopped")))?;
        result.await.map_err(worker_error)?
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
        self.inner.get_opts(location, options).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        self.inner
            .list(prefix)
            .try_filter(|meta| futures::future::ready(!is_publication_lock(&meta.location)))
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
    use object_store::{ObjectStoreExt, UpdateVersion};

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

    /// Same-size payloads, so only the inode and modification time can tell
    /// two versions' `ETag`s apart.
    fn versioned_payload(version: usize) -> PutPayload {
        PutPayload::from(format!("{version:08}").into_bytes())
    }

    /// Every version of a path carries an `ETag` no earlier version of it had,
    /// and an update built on any superseded version is refused. The check
    /// compares `ETag`s alone, so a repeat would let that update replace newer
    /// data. On ext4, a staged file reuses the inode the previous replacement
    /// freed, and versions written within one clock tick share a modification
    /// time, so without the floor version 2 repeats version 0's `ETag`.
    #[tokio::test]
    async fn a_superseded_etag_never_matches_a_later_version() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");
        let path = Path::from("test/versions.json");
        let versions = 1000;

        let mut etags = vec![
            store
                .put_opts(
                    &path,
                    versioned_payload(0),
                    PutOptions::from(PutMode::Create),
                )
                .await
                .expect("create version 0")
                .e_tag
                .expect("version 0 has an ETag"),
        ];
        let mut first_version_with = std::collections::HashMap::from([(etags[0].clone(), 0)]);
        for version in 1..versions {
            let mode = match version % 10 {
                // A key that is deleted and created again.
                0 => {
                    store.delete(&path).await.expect("delete");
                    PutMode::Create
                }
                5 => PutMode::Overwrite,
                _ => PutMode::Update(UpdateVersion {
                    e_tag: etags.last().cloned(),
                    version: None,
                }),
            };
            let etag = store
                .put_opts(&path, versioned_payload(version), PutOptions::from(mode))
                .await
                .unwrap_or_else(|error| panic!("write version {version}: {error}"))
                .e_tag
                .expect("every version has an ETag");
            if let Some(earlier) = first_version_with.insert(etag.clone(), version) {
                panic!("version {version} repeats the ETag {etag} of version {earlier}");
            }
            etags.push(etag);
        }

        for (version, stale) in etags.iter().enumerate().take(versions - 1) {
            let result = store
                .put_opts(
                    &path,
                    b"stale".as_slice().into(),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: Some(stale.clone()),
                        version: None,
                    })),
                )
                .await;
            assert!(
                matches!(result, Err(ObjectStoreError::Precondition { .. })),
                "an update built on version {version} must be refused, got {result:?}"
            );
        }
        let bytes = store
            .get(&path)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("read bytes");
        assert_eq!(bytes.as_ref(), format!("{:08}", versions - 1).as_bytes());
    }

    /// `put_opts` computes the `ETag` itself; it must be the one `head()` and
    /// `get()` report, or every conditional update after it would be refused.
    #[tokio::test]
    async fn put_returns_the_etag_head_reports() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LocalConditionalPut::new(dir.path()).expect("create store");
        let path = Path::from("test/etag.json");

        let created = store
            .put_opts(
                &path,
                versioned_payload(0),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create");
        let head = store.head(&path).await.expect("head after create");
        assert_eq!(created.e_tag, head.e_tag, "create");

        let overwritten = store
            .put_opts(
                &path,
                versioned_payload(1),
                PutOptions::from(PutMode::Overwrite),
            )
            .await
            .expect("overwrite");
        let head = store.head(&path).await.expect("head after overwrite");
        assert_eq!(overwritten.e_tag, head.e_tag, "overwrite");

        let updated = store
            .put_opts(
                &path,
                versioned_payload(2),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: overwritten.e_tag,
                    version: None,
                })),
            )
            .await
            .expect("update");
        let get = store.get(&path).await.expect("get after update");
        assert_eq!(updated.e_tag, get.meta.e_tag, "update");
        assert_eq!(get.bytes().await.expect("read bytes").as_ref(), b"00000002");
    }

    /// A staged file on a filesystem that stores whole seconds.
    struct WholeSecondFile(std::cell::Cell<SystemTime>);

    impl Timestamped for WholeSecondFile {
        fn modified(&self) -> io::Result<SystemTime> {
            Ok(self.0.get())
        }

        fn set_modified(&self, time: SystemTime) -> io::Result<()> {
            let seconds = time
                .duration_since(UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_secs();
            self.0.set(UNIX_EPOCH + Duration::from_secs(seconds));
            Ok(())
        }
    }

    /// On a filesystem whose timestamps cannot hold a time just past the
    /// floor, the version's time still moves past it without running ahead of
    /// the clock, which heartbeat and retention checks read as when the object
    /// was written.
    #[test]
    fn a_coarse_timestamp_waits_for_the_clock_instead_of_running_ahead() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after the epoch");
        let floor = UNIX_EPOCH + Duration::from_secs(now.as_secs());
        let staged = WholeSecondFile(std::cell::Cell::new(floor));

        stamp(&staged, floor).expect("stamp past the floor");

        let stored = staged.0.get();
        assert!(stored > floor, "stored {stored:?} is not past {floor:?}");
        assert!(
            stored <= SystemTime::now(),
            "stored {stored:?} is ahead of the clock"
        );
    }
}
