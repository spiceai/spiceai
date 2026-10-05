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

//! Detects whether a store enforces conditional writes.
//!
//! Shared state is safe only if the store refuses a write whose condition does
//! not hold. Most stores either enforce `If-None-Match`/`If-Match` or reject
//! them outright, which [`crate::conditional_put`] reports. The dangerous case
//! is a store that accepts the headers and ignores them: every conditional
//! write then succeeds, and concurrent writers silently overwrite each other.
//! Nothing short of trying tells that case apart, so [`probe_conditional_writes`]
//! makes one throwaway object and checks that the store refuses a duplicate
//! create and an update at a stale version.

use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};

/// Whether a store enforces one kind of conditional write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Enforcement {
    /// The store refuses a write whose condition does not hold.
    Enforced,
    /// The store accepted a write whose condition did not hold: it ignores
    /// the condition, so concurrent writers overwrite each other.
    Ignored,
    /// The store rejects this kind of conditional write.
    Unsupported,
    /// The probe could not tell, for example because it lacked permission to
    /// write its probe object. Carries the reason.
    Unknown(String),
}

/// What [`probe_conditional_writes`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionalWriteSupport {
    /// Create-if-absent (`If-None-Match: *`).
    pub create: Enforcement,
    /// Update-at-version (`If-Match`).
    pub update: Enforcement,
}

impl ConditionalWriteSupport {
    /// Both kinds of conditional write are enforced.
    #[must_use]
    pub fn is_enforced(&self) -> bool {
        self.create == Enforcement::Enforced && self.update == Enforcement::Enforced
    }

    /// The store was seen accepting a write whose condition did not hold.
    #[must_use]
    pub fn ignores_conditions(&self) -> bool {
        self.create == Enforcement::Ignored || self.update == Enforcement::Ignored
    }

    /// The probe told, for both kinds of conditional write, whether the store
    /// enforces, ignores or rejects the condition.
    #[must_use]
    pub fn is_conclusive(&self) -> bool {
        !matches!(self.create, Enforcement::Unknown(_))
            && !matches!(self.update, Enforcement::Unknown(_))
    }

    /// Why the probe could not tell, or `None` when it told for both kinds of
    /// conditional write. Names the first kind left [`Enforcement::Unknown`].
    #[must_use]
    pub fn inconclusive_reason(&self) -> Option<&str> {
        [&self.create, &self.update]
            .into_iter()
            .find_map(|enforcement| match enforcement {
                Enforcement::Unknown(reason) => Some(reason.as_str()),
                Enforcement::Enforced | Enforcement::Ignored | Enforcement::Unsupported => None,
            })
    }

    /// Why shared state must not be kept in this store, worded to follow "the
    /// store …", or `None` when the probe found nothing that rules it out.
    #[must_use]
    pub fn refusal_reason(&self) -> Option<&'static str> {
        if self.ignores_conditions() {
            Some("accepted a write whose If-None-Match or If-Match condition did not hold")
        } else if self.create == Enforcement::Unsupported || self.update == Enforcement::Unsupported
        {
            Some("does not support conditional writes")
        } else {
            None
        }
    }
}

/// Probes `store` by writing, rewriting and deleting one object under
/// `prefix`, named `.spice-conditional-write-probe-<uuid>`.
///
/// Makes at most five requests. The probe object is deleted before returning;
/// a failed delete leaves one small object behind and does not change the
/// result.
pub async fn probe_conditional_writes(
    store: &dyn ObjectStore,
    prefix: &Path,
) -> ConditionalWriteSupport {
    let location = prefix.clone().join(format!(
        ".spice-conditional-write-probe-{}",
        uuid::Uuid::now_v7().simple()
    ));
    let support = run_probe(store, &location).await;
    if let Err(err) = store.delete(&location).await
        && !matches!(err, ObjectStoreError::NotFound { .. })
    {
        tracing::debug!("Failed to delete the conditional-write probe object {location}: {err}");
    }
    support
}

async fn run_probe(store: &dyn ObjectStore, location: &Path) -> ConditionalWriteSupport {
    let unknown = |reason: String| ConditionalWriteSupport {
        create: Enforcement::Unknown(reason.clone()),
        update: Enforcement::Unknown(reason),
    };

    // 1. Create the probe object. Only a store that reports it cannot create
    //    conditionally at all falls back to a plain write, so the update
    //    check still has an object to work with.
    let (first, create_supported) = match put(store, location, "probe-1", PutMode::Create).await {
        Ok(version) => (version, true),
        Err(
            err @ (ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented { .. }),
        ) => {
            tracing::debug!("Conditional create is unsupported at {location}: {err}");
            match put(store, location, "probe-1", PutMode::Overwrite).await {
                Ok(version) => (version, false),
                Err(err) => return unknown(format!("could not write a probe object: {err}")),
            }
        }
        Err(err) => return unknown(format!("could not write a probe object: {err}")),
    };

    // 2. A second create of the same object must be refused. A store that
    //    accepts it has moved the object to the version it returns.
    let (create, current) = if create_supported {
        match put(store, location, "probe-2", PutMode::Create).await {
            Err(ObjectStoreError::AlreadyExists { .. } | ObjectStoreError::Precondition { .. }) => {
                (Enforcement::Enforced, first)
            }
            Ok(version) => (Enforcement::Ignored, version),
            Err(
                ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented { .. },
            ) => (Enforcement::Unsupported, first),
            Err(err) => (
                Enforcement::Unknown(format!("a duplicate create failed with: {err}")),
                first,
            ),
        }
    } else {
        (Enforcement::Unsupported, first)
    };

    // 3. An update at the current version must land, and 4. repeating it, now
    //    at a stale version, must be refused.
    let update = match put(store, location, "probe-3", PutMode::Update(current.clone())).await {
        Ok(_) => match put(store, location, "probe-4", PutMode::Update(current)).await {
            Err(ObjectStoreError::Precondition { .. }) => Enforcement::Enforced,
            Ok(_) => Enforcement::Ignored,
            Err(err) => Enforcement::Unknown(format!("a stale update failed with: {err}")),
        },
        Err(ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented { .. }) => {
            Enforcement::Unsupported
        }
        Err(err) => Enforcement::Unknown(format!(
            "an update at the current version failed with: {err}"
        )),
    };

    ConditionalWriteSupport { create, update }
}

async fn put(
    store: &dyn ObjectStore,
    location: &Path,
    body: &'static str,
    mode: PutMode,
) -> Result<UpdateVersion, ObjectStoreError> {
    store
        .put_opts(location, body.into(), PutOptions::from(mode))
        .await
        .map(UpdateVersion::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutPayload, PutResult, RenameOptions,
    };
    use std::sync::Arc;

    fn prefix() -> Path {
        Path::from("state")
    }

    async fn assert_probe_leaves_nothing(store: &dyn ObjectStore) {
        use futures::TryStreamExt;
        let listed: Vec<_> = store
            .list(Some(&prefix()))
            .try_collect()
            .await
            .expect("list");
        assert!(listed.is_empty(), "probe object left behind: {listed:?}");
    }

    #[tokio::test]
    async fn in_memory_enforces_both() {
        let store = InMemory::new();
        let support = probe_conditional_writes(&store, &prefix()).await;
        assert!(support.is_enforced(), "{support:?}");
        assert_probe_leaves_nothing(&store).await;
    }

    #[tokio::test]
    async fn local_conditional_put_enforces_both() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = crate::LocalConditionalPut::new(dir.path()).expect("store");
        let support = probe_conditional_writes(&store, &prefix()).await;
        assert!(support.is_enforced(), "{support:?}");
    }

    #[tokio::test]
    async fn plain_local_filesystem_cannot_update() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            object_store::local::LocalFileSystem::new_with_prefix(dir.path()).expect("local fs");
        let support = probe_conditional_writes(&store, &prefix()).await;
        assert_eq!(support.create, Enforcement::Enforced, "{support:?}");
        assert_eq!(support.update, Enforcement::Unsupported, "{support:?}");
        assert!(!support.is_enforced());
    }

    /// A store that accepts `If-None-Match`/`If-Match` and ignores them — the
    /// S3-compatible failure the probe exists to catch.
    #[derive(Debug)]
    struct IgnoresConditions(Arc<InMemory>);

    impl std::fmt::Display for IgnoresConditions {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "IgnoresConditions")
        }
    }

    #[async_trait]
    impl ObjectStore for IgnoresConditions {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let opts = PutOptions {
                mode: PutMode::Overwrite,
                ..opts
            };
            self.0.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.0.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.0.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.0.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.0.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.0.rename_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn a_store_that_ignores_conditions_is_caught() {
        let store = IgnoresConditions(Arc::new(InMemory::new()));
        let support = probe_conditional_writes(&store, &prefix()).await;
        assert_eq!(support.create, Enforcement::Ignored, "{support:?}");
        assert_eq!(support.update, Enforcement::Ignored, "{support:?}");
        assert!(support.ignores_conditions());
        assert!(!support.is_enforced());
    }

    /// A partial probe that already saw Ignored or Unsupported is enough to refuse the
    /// store, even when the other check is still Unknown. Callers that cache only
    /// `is_conclusive()` would keep re-probing; they must also cache when
    /// `refusal_reason()` is set.
    #[test]
    fn a_partial_refusal_is_cacheable_even_when_inconclusive() {
        let ignored_unknown = ConditionalWriteSupport {
            create: Enforcement::Ignored,
            update: Enforcement::Unknown("update not tried".into()),
        };
        assert!(!ignored_unknown.is_conclusive(), "{ignored_unknown:?}");
        assert_eq!(
            ignored_unknown.refusal_reason(),
            Some("accepted a write whose If-None-Match or If-Match condition did not hold"),
            "{ignored_unknown:?}"
        );

        let unsupported_unknown = ConditionalWriteSupport {
            create: Enforcement::Unsupported,
            update: Enforcement::Unknown("update not tried".into()),
        };
        assert!(
            !unsupported_unknown.is_conclusive(),
            "{unsupported_unknown:?}"
        );
        assert_eq!(
            unsupported_unknown.refusal_reason(),
            Some("does not support conditional writes"),
            "{unsupported_unknown:?}"
        );

        let unknown_unknown = ConditionalWriteSupport {
            create: Enforcement::Unknown("create failed".into()),
            update: Enforcement::Unknown("update not tried".into()),
        };
        assert!(!unknown_unknown.is_conclusive(), "{unknown_unknown:?}");
        assert!(
            unknown_unknown.refusal_reason().is_none(),
            "genuinely unknown must keep retrying: {unknown_unknown:?}"
        );
    }
}
