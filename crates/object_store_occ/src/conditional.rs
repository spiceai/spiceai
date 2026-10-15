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

//! Conditional writes that never degrade into unconditional ones.
//!
//! A compare-and-swap on shared state is only as good as its weakest write. A
//! writer that falls back to [`PutMode::Overwrite`] when the store will not
//! honor a condition silently turns "exactly one writer wins" into "the last
//! writer wins", losing every concurrent update it overwrites. [`conditional_put`]
//! therefore has no fallback: a write lands only under its condition, and a
//! store that cannot enforce the condition is reported as
//! [`ConditionalWriteError::Unsupported`] for the caller to surface.

use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, ObjectStore, PutMode, PutOptions, PutPayload, PutResult,
    UpdateVersion,
};
use snafu::Snafu;

/// The state an object must be in for a conditional write to land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expected {
    /// The object must not exist (`If-None-Match: *`).
    Absent,
    /// The object must still be at this version (`If-Match`).
    Version(UpdateVersion),
}

impl Expected {
    /// The expectation for an object last read at `version`.
    ///
    /// Returns `None` when the read reported neither an `ETag` nor a version:
    /// such an object cannot be updated conditionally, and writing it
    /// unconditionally could overwrite a concurrent change.
    #[must_use]
    pub fn at_version(version: UpdateVersion) -> Option<Self> {
        if version.e_tag.is_none() && version.version.is_none() {
            None
        } else {
            Some(Self::Version(version))
        }
    }
}

/// Why a conditional write did not land.
#[derive(Debug, Snafu)]
pub enum ConditionalWriteError {
    /// Another writer created or changed the object first. Re-read it and
    /// retry the change against what is there now.
    #[snafu(display("Another writer changed '{path}' first"))]
    Conflict { path: String },

    /// The store cannot enforce the condition, so this write could overwrite a
    /// concurrent change. The caller must not fall back to an unconditional
    /// write.
    #[snafu(display(
        "The object store cannot write '{path}' conditionally, so concurrent writers could overwrite each other's changes. Use a store that supports conditional writes (If-None-Match and If-Match), such as Amazon S3, Azure Blob Storage, Google Cloud Storage or a local directory. Cause: {source}"
    ))]
    Unsupported {
        path: String,
        source: ObjectStoreError,
    },

    /// The write failed for another reason: network, permissions, and so on.
    #[snafu(display("Failed to write '{path}': {source}"))]
    Store {
        path: String,
        source: ObjectStoreError,
    },
}

impl ConditionalWriteError {
    /// Whether another writer won the race.
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }
}

/// Writes `payload` to `path` only if the object is in the `expected` state.
///
/// Maps the store's refusal onto [`ConditionalWriteError::Conflict`], and a
/// store that cannot evaluate the condition onto
/// [`ConditionalWriteError::Unsupported`]. Never retries and never falls back
/// to an unconditional write.
///
/// # Errors
///
/// Returns [`ConditionalWriteError`] when the write did not land.
pub async fn conditional_put(
    store: &dyn ObjectStore,
    path: &Path,
    payload: PutPayload,
    expected: &Expected,
) -> Result<PutResult, ConditionalWriteError> {
    let mode = match expected {
        Expected::Absent => PutMode::Create,
        Expected::Version(version) => PutMode::Update(version.clone()),
    };

    match store.put_opts(path, payload, PutOptions::from(mode)).await {
        Ok(result) => Ok(result),
        Err(err) => Err(classify(path, expected, err)),
    }
}

/// Classifies the error a conditional write returned.
fn classify(path: &Path, expected: &Expected, err: ObjectStoreError) -> ConditionalWriteError {
    let path = path.to_string();
    match (expected, err) {
        (Expected::Absent, ObjectStoreError::AlreadyExists { .. })
        | (_, ObjectStoreError::Precondition { .. })
        // An `If-Match` write to an object deleted since it was read: the
        // version the writer read no longer exists.
        | (Expected::Version(_), ObjectStoreError::NotFound { .. }) => {
            ConditionalWriteError::Conflict { path }
        }
        (_, source @ (ObjectStoreError::NotSupported { .. }
        | ObjectStoreError::NotImplemented { .. })) => {
            ConditionalWriteError::Unsupported { path, source }
        }
        (_, source) => ConditionalWriteError::Store { path, source },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    fn path() -> Path {
        Path::from("state/doc.json")
    }

    #[tokio::test]
    async fn absent_creates_once_then_conflicts() {
        let store = InMemory::new();
        conditional_put(&store, &path(), "a".into(), &Expected::Absent)
            .await
            .expect("first create lands");
        let err = conditional_put(&store, &path(), "b".into(), &Expected::Absent)
            .await
            .expect_err("second create must conflict");
        assert!(err.is_conflict(), "{err:?}");
        let bytes = store
            .get(&path())
            .await
            .expect("object exists")
            .bytes()
            .await
            .expect("read body");
        assert_eq!(&bytes[..], b"a", "the losing create must not overwrite");
    }

    #[tokio::test]
    async fn stale_version_conflicts_and_current_version_lands() {
        let store = InMemory::new();
        let first = conditional_put(&store, &path(), "a".into(), &Expected::Absent)
            .await
            .expect("create lands");
        let v1 = Expected::at_version(UpdateVersion::from(first)).expect("in-memory has an ETag");
        let second = conditional_put(&store, &path(), "b".into(), &v1)
            .await
            .expect("update at the current version lands");

        let err = conditional_put(&store, &path(), "c".into(), &v1)
            .await
            .expect_err("update at a stale version must conflict");
        assert!(err.is_conflict(), "{err:?}");

        let v2 = Expected::at_version(UpdateVersion::from(second)).expect("ETag");
        conditional_put(&store, &path(), "d".into(), &v2)
            .await
            .expect("update at the new version lands");
    }

    #[tokio::test]
    async fn update_of_a_deleted_object_conflicts() {
        let store = InMemory::new();
        let first = conditional_put(&store, &path(), "a".into(), &Expected::Absent)
            .await
            .expect("create lands");
        store.delete(&path()).await.expect("delete");
        let expected = Expected::at_version(UpdateVersion::from(first)).expect("ETag");
        let err = conditional_put(&store, &path(), "b".into(), &expected)
            .await
            .expect_err("the object the writer read is gone");
        assert!(err.is_conflict(), "{err:?}");
    }

    #[test]
    fn a_read_without_etag_or_version_cannot_be_updated_conditionally() {
        assert_eq!(
            Expected::at_version(UpdateVersion {
                e_tag: None,
                version: None,
            }),
            None
        );
    }

    #[tokio::test]
    async fn local_filesystem_update_is_unsupported_not_overwritten() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            object_store::local::LocalFileSystem::new_with_prefix(dir.path()).expect("local fs");
        let first = conditional_put(&store, &path(), "a".into(), &Expected::Absent)
            .await
            .expect("local create is atomic");
        let expected = Expected::at_version(UpdateVersion::from(first)).expect("ETag");
        let err = conditional_put(&store, &path(), "b".into(), &expected)
            .await
            .expect_err("LocalFileSystem cannot update conditionally");
        assert!(
            matches!(err, ConditionalWriteError::Unsupported { .. }),
            "{err:?}"
        );
        let bytes = store
            .get(&path())
            .await
            .expect("object exists")
            .bytes()
            .await
            .expect("read body");
        assert_eq!(&bytes[..], b"a", "an unsupported update must not write");
    }
}
