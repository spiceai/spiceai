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

//! The startup check of a `runtime.state` location.
//!
//! Instances that share a location are safe only if its store refuses a write
//! whose condition does not hold. Every instance with `runtime.state` runs
//! [`check_location`] at startup, and the location's writers wait in
//! [`StateLocationCheck::guard`] until it has passed.

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions,
};
use object_store_occ::{Enforcement, probe_conditional_writes};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio::sync::watch;

use crate::StateFeature;

/// The version of the layout Spice writes in a state location. `_spice/format.json`
/// and the `format-version` of every snapshot `metadata.json` carry this number.
pub const FORMAT_VERSION: u32 = 1;

/// The object in a `runtime.state` location's `_spice/` folder that records its
/// [`FORMAT_VERSION`].
pub const FORMAT_FILE: &str = "format.json";

pub const RUNTIME_STATE_DOCS: &str =
    "https://spiceai.org/docs/reference/spicepod/runtime#runtimestate";

const SAFE_STORES: &str = "Use a store that enforces conditional writes (Amazon S3, Azure Blob Storage, Google Cloud Storage, or a local directory).";

#[derive(Debug, Serialize, Deserialize)]
struct FormatRecord {
    #[serde(rename = "format-version")]
    format_version: u32,
}

/// Why a `runtime.state` location did not pass its startup check.
#[derive(Debug, Snafu)]
pub enum CheckError {
    #[snafu(display(
        "Shared state location '{location}' accepted a write whose {condition} condition did not hold, so instances sharing it could overwrite each other's state. {SAFE_STORES} See: {RUNTIME_STATE_DOCS}"
    ))]
    ConditionsIgnored {
        location: String,
        condition: &'static str,
    },

    #[snafu(display(
        "Shared state location '{location}' does not support conditional writes, so instances sharing it could overwrite each other's state. {SAFE_STORES} See: {RUNTIME_STATE_DOCS}"
    ))]
    ConditionsUnsupported { location: String },

    #[snafu(display(
        "Shared state location '{location}' has format version {version}, and this version of Spice supports up to version {FORMAT_VERSION}. Upgrade Spice to a version that supports state format version {version}. See: {RUNTIME_STATE_DOCS}"
    ))]
    NewerFormat { location: String, version: u32 },

    #[snafu(display(
        "Shared state location '{location}' has an invalid '{path}', which must hold {{\"format-version\": {FORMAT_VERSION}}}: {source}. See: {RUNTIME_STATE_DOCS}"
    ))]
    InvalidFormatFile {
        location: String,
        path: Path,
        source: serde_json::Error,
    },

    /// The check could not finish, for example because the store refused a
    /// request or could not be reached. Running it again can pass.
    #[snafu(display("{cause}"))]
    Unfinished { cause: String },
}

impl CheckError {
    /// Whether running the check again can pass.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unfinished { .. })
    }

    #[must_use]
    pub fn unfinished(cause: impl Into<String>) -> Self {
        Self::Unfinished {
            cause: cause.into(),
        }
    }
}

/// Checks the `runtime.state` location `location`, whose `_spice/` folder is
/// `prefix` in `store`: its `format.json` is not newer than [`FORMAT_VERSION`],
/// and the store refuses a write whose condition does not hold. Creates
/// `format.json` when it is missing, with a create-only write.
///
/// # Errors
///
/// Returns [`CheckError::Unfinished`] when the check could not finish, and
/// another variant when the location must not be used.
pub async fn check_location(
    store: &dyn ObjectStore,
    prefix: &Path,
    location: &str,
) -> Result<(), CheckError> {
    let format_path = prefix.clone().join(FORMAT_FILE);
    let recorded = read_format_version(store, &format_path, location).await?;

    let support = probe_conditional_writes(store, prefix).await;
    if support.update == Enforcement::Ignored {
        return ConditionsIgnoredSnafu {
            location,
            condition: "If-Match",
        }
        .fail();
    }
    if support.create == Enforcement::Ignored {
        return ConditionsIgnoredSnafu {
            location,
            condition: "If-None-Match",
        }
        .fail();
    }
    if support.create == Enforcement::Unsupported || support.update == Enforcement::Unsupported {
        return ConditionsUnsupportedSnafu { location }.fail();
    }
    if let Some(reason) = support.inconclusive_reason() {
        return Err(CheckError::unfinished(reason));
    }

    if recorded.is_none() {
        create_format_file(store, &format_path, location).await?;
    }
    Ok(())
}

/// The version `format.json` records, or `None` when there is no `format.json`.
async fn read_format_version(
    store: &dyn ObjectStore,
    path: &Path,
    location: &str,
) -> Result<Option<u32>, CheckError> {
    let bytes = match store.get(path).await {
        Ok(result) => result.bytes().await,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(err) => Err(err),
    }
    .map_err(|err| CheckError::unfinished(format!("could not read '{path}': {err}")))?;
    let record: FormatRecord = serde_json::from_slice(&bytes).context(InvalidFormatFileSnafu {
        location,
        path: path.clone(),
    })?;
    ensure!(
        record.format_version <= FORMAT_VERSION,
        NewerFormatSnafu {
            location,
            version: record.format_version,
        }
    );
    Ok(Some(record.format_version))
}

async fn create_format_file(
    store: &dyn ObjectStore,
    path: &Path,
    location: &str,
) -> Result<(), CheckError> {
    let record = serde_json::to_vec(&FormatRecord {
        format_version: FORMAT_VERSION,
    })
    .map_err(|err| CheckError::unfinished(format!("could not encode '{path}': {err}")))?;
    match store
        .put_opts(path, record.into(), PutOptions::from(PutMode::Create))
        .await
    {
        Ok(_) => Ok(()),
        // Another instance created it first: its version must be one this
        // instance supports, too.
        Err(ObjectStoreError::AlreadyExists { .. } | ObjectStoreError::Precondition { .. }) => {
            read_format_version(store, path, location).await.map(|_| ())
        }
        Err(err) => Err(CheckError::unfinished(format!(
            "could not create '{path}': {err}"
        ))),
    }
}

#[derive(Debug, Clone)]
enum Outcome {
    Pending,
    Passed,
    Refused(Arc<str>),
}

/// The outcome of one `runtime.state` location's startup check, and the
/// features that keep state in it.
#[derive(Debug)]
pub struct StateLocationCheck {
    location: String,
    outcome: watch::Sender<Outcome>,
    features: parking_lot::Mutex<BTreeSet<StateFeature>>,
    guarded: parking_lot::Mutex<BTreeSet<StateFeature>>,
}

impl StateLocationCheck {
    #[must_use]
    pub fn new(location: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            location: location.into(),
            outcome: watch::Sender::new(Outcome::Pending),
            features: parking_lot::Mutex::default(),
            guarded: parking_lot::Mutex::default(),
        })
    }

    #[must_use]
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Records that `feature` keeps state in the location.
    pub fn record_feature(&self, feature: StateFeature) {
        self.features.lock().insert(feature);
    }

    /// The features recorded with [`Self::record_feature`] or [`Self::guard`].
    #[must_use]
    pub fn features(&self) -> Vec<StateFeature> {
        self.features.lock().iter().copied().collect()
    }

    /// The features whose writes wait for the check: those recorded with
    /// [`Self::guard`].
    #[must_use]
    pub fn guarded_features(&self) -> Vec<StateFeature> {
        self.guarded.lock().iter().copied().collect()
    }

    /// Wraps `feature`'s store in the location so that its writes wait until
    /// the check has passed, and fail once it has refused the location. Reads
    /// do not wait.
    #[must_use]
    pub fn guard(
        self: &Arc<Self>,
        feature: StateFeature,
        store: Arc<dyn ObjectStore>,
    ) -> Arc<dyn ObjectStore> {
        self.record_feature(feature);
        self.guarded.lock().insert(feature);
        Arc::new(GuardedStore {
            inner: store,
            check: Arc::clone(self),
        })
    }

    pub fn pass(&self) {
        self.outcome.send_replace(Outcome::Passed);
    }

    /// Refuses the location for `reason`: guarded writes fail with it.
    pub fn refuse(&self, reason: impl Into<Arc<str>>) {
        self.outcome.send_replace(Outcome::Refused(reason.into()));
    }

    /// Waits until the check has finished. Returns why it refused the
    /// location, or `None` when it passed.
    pub async fn finished(&self) -> Option<Arc<str>> {
        let mut outcome = self.outcome.subscribe();
        // `self` holds the sender, so the channel stays open while this waits.
        let finished = outcome
            .wait_for(|outcome| !matches!(outcome, Outcome::Pending))
            .await
            .ok()?;
        match &*finished {
            Outcome::Refused(reason) => Some(Arc::clone(reason)),
            Outcome::Pending | Outcome::Passed => None,
        }
    }

    async fn wait_until_passed(&self) -> Result<(), ObjectStoreError> {
        match self.finished().await {
            None => Ok(()),
            Some(reason) => Err(ObjectStoreError::Generic {
                store: "runtime.state",
                source: reason.to_string().into(),
            }),
        }
    }
}

/// A store whose writes wait for a [`StateLocationCheck`] to pass.
#[derive(Debug)]
struct GuardedStore {
    inner: Arc<dyn ObjectStore>,
    check: Arc<StateLocationCheck>,
}

impl Display for GuardedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl ObjectStore for GuardedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        self.check.wait_until_passed().await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        self.check.wait_until_passed().await?;
        self.inner.put_multipart_opts(location, opts).await
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
        let inner = Arc::clone(&self.inner);
        let check = Arc::clone(&self.check);
        futures::stream::once(async move {
            match check.wait_until_passed().await {
                Ok(()) => inner.delete_stream(locations),
                Err(err) => futures::stream::iter([Err(err)]).boxed(),
            }
        })
        .flatten()
        .boxed()
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
        self.check.wait_until_passed().await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<(), ObjectStoreError> {
        self.check.wait_until_passed().await?;
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    const LOCATION: &str = "s3://my-bucket/spice/";

    fn prefix() -> Path {
        Path::from("spice/_spice")
    }

    fn format_path() -> Path {
        prefix().join(FORMAT_FILE)
    }

    async fn put_format(store: &dyn ObjectStore, body: &'static str) {
        store
            .put(&format_path(), body.into())
            .await
            .expect("write format.json");
    }

    async fn read_format(store: &dyn ObjectStore) -> serde_json::Value {
        let bytes = store
            .get(&format_path())
            .await
            .expect("format.json exists")
            .bytes()
            .await
            .expect("read format.json");
        serde_json::from_slice(&bytes).expect("format.json is JSON")
    }

    async fn listed(store: &dyn ObjectStore) -> Vec<Path> {
        store
            .list(None)
            .map_ok(|meta| meta.location)
            .try_collect()
            .await
            .expect("list")
    }

    /// Accepts `If-None-Match`/`If-Match` and ignores them, or fails every write
    /// while `fail_writes` is set.
    #[derive(Debug, Default)]
    struct TestStore {
        inner: InMemory,
        ignore_conditions: bool,
        fail_writes: AtomicBool,
    }

    impl Display for TestStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestStore")
        }
    }

    impl TestStore {
        fn ignoring_conditions() -> Self {
            Self {
                ignore_conditions: true,
                ..Self::default()
            }
        }

        fn failing_writes() -> Self {
            Self {
                fail_writes: AtomicBool::new(true),
                ..Self::default()
            }
        }

        fn write_error(&self) -> Result<(), ObjectStoreError> {
            if self.fail_writes.load(Ordering::SeqCst) {
                return Err(ObjectStoreError::PermissionDenied {
                    path: String::new(),
                    source: "Access Denied".into(),
                });
            }
            Ok(())
        }
    }

    #[async_trait]
    impl ObjectStore for TestStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult, ObjectStoreError> {
            self.write_error()?;
            let opts = if self.ignore_conditions {
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..opts
                }
            } else {
                opts
            };
            self.inner.put_opts(location, payload, opts).await
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
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn creates_format_file_when_missing_and_leaves_only_it() {
        let store = InMemory::new();
        check_location(&store, &prefix(), LOCATION)
            .await
            .expect("an empty location passes");

        assert_eq!(
            read_format(&store).await,
            serde_json::json!({ "format-version": FORMAT_VERSION })
        );
        assert_eq!(listed(&store).await, [format_path()]);
    }

    #[tokio::test]
    async fn accepts_format_file_at_the_supported_version() {
        let store = InMemory::new();
        put_format(&store, r#"{"format-version": 1}"#).await;
        check_location(&store, &prefix(), LOCATION)
            .await
            .expect("version 1 passes");
        assert_eq!(read_format(&store).await["format-version"], 1);
    }

    #[tokio::test]
    async fn concurrent_checks_create_one_format_file() {
        let store = InMemory::new();
        let prefix = prefix();
        let (first, second) = tokio::join!(
            check_location(&store, &prefix, LOCATION),
            check_location(&store, &prefix, LOCATION),
        );
        first.expect("first check passes");
        second.expect("second check passes");
        assert_eq!(listed(&store).await, [format_path()]);
    }

    #[tokio::test]
    async fn refuses_a_newer_format_file_naming_the_upgrade() {
        let store = InMemory::new();
        put_format(&store, r#"{"format-version": 2}"#).await;
        let err = check_location(&store, &prefix(), LOCATION)
            .await
            .expect_err("version 2 is refused");

        assert!(matches!(err, CheckError::NewerFormat { version: 2, .. }));
        assert!(!err.is_retryable());
        assert_eq!(
            err.to_string(),
            format!(
                "Shared state location '{LOCATION}' has format version 2, and this version of Spice supports up to version 1. Upgrade Spice to a version that supports state format version 2. See: {RUNTIME_STATE_DOCS}"
            )
        );
    }

    #[tokio::test]
    async fn refuses_an_invalid_format_file() {
        let store = InMemory::new();
        put_format(&store, "not json").await;
        let err = check_location(&store, &prefix(), LOCATION)
            .await
            .expect_err("an unreadable format.json is refused");
        assert!(matches!(err, CheckError::InvalidFormatFile { .. }));
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn refuses_a_store_that_ignores_conditions_with_the_spec_error() {
        let store = TestStore::ignoring_conditions();
        let err = check_location(&store, &prefix(), LOCATION)
            .await
            .expect_err("a store that ignores conditions is refused");

        assert!(!err.is_retryable());
        assert_eq!(
            err.to_string(),
            "Shared state location 's3://my-bucket/spice/' accepted a write whose If-Match condition did not hold, so instances sharing it could overwrite each other's state. Use a store that enforces conditional writes (Amazon S3, Azure Blob Storage, Google Cloud Storage, or a local directory). See: https://spiceai.org/docs/reference/spicepod/runtime#runtimestate"
        );
        assert!(
            listed(&store).await.is_empty(),
            "no format.json on a refused store"
        );
    }

    #[tokio::test]
    async fn a_store_that_refuses_writes_can_be_checked_again() {
        let store = TestStore::failing_writes();
        let err = check_location(&store, &prefix(), LOCATION)
            .await
            .expect_err("a store without write access cannot be checked");
        assert!(err.is_retryable(), "{err}");
        assert!(err.to_string().contains("Access Denied"), "{err}");

        store.fail_writes.store(false, Ordering::SeqCst);
        check_location(&store, &prefix(), LOCATION)
            .await
            .expect("the check passes once writes are allowed");
        assert_eq!(read_format(&store).await["format-version"], FORMAT_VERSION);
    }

    #[tokio::test]
    async fn guarded_writes_wait_for_the_check_and_reads_do_not() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        inner
            .put(&Path::from("cluster/cluster.json"), "{}".into())
            .await
            .expect("seed");
        let check = StateLocationCheck::new(LOCATION);
        let store = check.guard(StateFeature::Scheduler, Arc::clone(&inner));

        store
            .get(&Path::from("cluster/cluster.json"))
            .await
            .expect("reads do not wait");

        let write = tokio::spawn({
            let store = Arc::clone(&store);
            async move { store.put(&Path::from("cluster/a.json"), "a".into()).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!write.is_finished(), "the write waits for the check");
        assert!(
            matches!(
                inner.head(&Path::from("cluster/a.json")).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "the write has not reached the store"
        );

        check.pass();
        write
            .await
            .expect("join")
            .expect("the write lands once the check passes");
        assert_eq!(check.features(), [StateFeature::Scheduler]);
        assert_eq!(check.guarded_features(), [StateFeature::Scheduler]);
    }

    #[tokio::test]
    async fn guarded_writes_fail_once_the_check_refuses() {
        let check = StateLocationCheck::new(LOCATION);
        let store = check.guard(StateFeature::RateControl, Arc::new(InMemory::new()));
        check.refuse("the store ignores conditions");

        let err = store
            .put(&Path::from("rate-control/a.json"), "a".into())
            .await
            .expect_err("a refused location takes no writes");
        assert!(err.to_string().contains("the store ignores conditions"));
        let deleted: Vec<_> = store
            .delete_stream(futures::stream::iter([Ok(Path::from("a"))]).boxed())
            .collect()
            .await;
        assert!(matches!(deleted.as_slice(), [Err(_)]));
        assert_eq!(
            check.finished().await.as_deref(),
            Some("the store ignores conditions")
        );
    }
}
