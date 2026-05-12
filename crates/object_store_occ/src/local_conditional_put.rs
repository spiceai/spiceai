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
//! checking the current `ETag` under a file-backed advisory lock, then writing
//! with `PutMode::Overwrite` if the `ETag` matches.
//!
//! **Concurrency model**: a lock file under the local state root serializes
//! conditional writes across [`LocalConditionalPut`] instances and across local
//! processes that use this wrapper for the same directory.
//!
//! [`LocalFileSystem`]: object_store::local::LocalFileSystem
//! [`Error::NotImplemented`]: object_store::Error::NotImplemented

use std::fmt::{self, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Wraps a [`LocalFileSystem`] to add `PutMode::Update` support.
///
/// For `PutMode::Update`, it:
/// 1. Acquires a file-backed advisory lock for the local state root
/// 2. Reads the current `ETag` of the target file via `head()`
/// 3. Compares with the expected `ETag` from the [`UpdateVersion`]
/// 4. Writes with `PutMode::Overwrite` if they match
/// 5. Releases the advisory lock
///
/// All other operations are delegated directly to the inner `LocalFileSystem`.
///
/// [`UpdateVersion`]: object_store::UpdateVersion
pub struct LocalConditionalPut {
    inner: LocalFileSystem,
    root: PathBuf,
    semaphore: LocalFileSemaphore,
}

#[derive(Debug, Clone)]
struct LocalFileSemaphore {
    lock_path: PathBuf,
}

struct LocalFileSemaphoreGuard {
    _file: File,
}

impl LocalFileSemaphore {
    fn new(root: &std::path::Path) -> Self {
        Self {
            lock_path: root.join(".spice-object-store-occ.lock"),
        }
    }

    async fn acquire(&self) -> Result<LocalFileSemaphoreGuard, ObjectStoreError> {
        let lock_path = self.lock_path.clone();
        let file = tokio::task::spawn_blocking(move || -> io::Result<File> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)?;
            file.lock()?;
            Ok(file)
        })
        .await
        .map_err(|source| ObjectStoreError::Generic {
            store: "LocalConditionalPut",
            source: Box::new(source),
        })?
        .map_err(|source| ObjectStoreError::Generic {
            store: "LocalConditionalPut",
            source: Box::new(source),
        })?;

        Ok(LocalFileSemaphoreGuard { _file: file })
    }
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
        let inner = LocalFileSystem::new_with_prefix(&root)?;
        let semaphore = LocalFileSemaphore::new(&root);
        Ok(Self {
            inner,
            root,
            semaphore,
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
        if let PutMode::Update(expected) = &opts.mode {
            let expected = expected.clone();
            let _guard = self.semaphore.acquire().await?;

            // Read current ETag.
            // Note: only `e_tag` is compared because `LocalFileSystem` does not
            // support object versioning — `PutResult::version` is always `None`.
            let current_meta = self.inner.head(location).await?;

            if current_meta.e_tag != expected.e_tag {
                return Err(ObjectStoreError::Precondition {
                    path: location.to_string(),
                    source: format!(
                        "ETag mismatch: expected {:?}, found {:?}",
                        expected.e_tag, current_meta.e_tag
                    )
                    .into(),
                });
            }

            // ETag matches — write with overwrite mode.
            let overwrite_opts = PutOptions {
                mode: PutMode::Overwrite,
                ..opts
            };
            self.inner.put_opts(location, payload, overwrite_opts).await
        } else {
            self.inner.put_opts(location, payload, opts).await
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta, ObjectStoreError> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<(), ObjectStoreError> {
        self.inner.delete(location).await
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

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), ObjectStoreError> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<(), ObjectStoreError> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<(), ObjectStoreError> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
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
