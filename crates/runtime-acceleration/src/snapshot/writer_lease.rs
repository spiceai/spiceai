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

//! Elects one snapshot writer per dataset among the instances that create
//! snapshots in the same location.
//!
//! The instances race for a lease object, `<location>/leases/<dataset>.json`,
//! with the store's conditional writes. An absent lease is created with
//! `If-None-Match: *`, and a held lease is renewed or taken over with
//! `If-Match` on the version that was read, so exactly one write wins each
//! race. The holder renews the lease each time it is about to create a
//! snapshot, and every other instance skips its snapshots while the lease is
//! live.
//!
//! A lease lapses once one version of it has stayed unchanged, on the
//! observing instance's own clock, for the duration its holder wrote into it:
//! twice the dataset's snapshot interval, kept between [`MIN_LEASE_DURATION`]
//! and [`MAX_LEASE_DURATION`]. Neither the store's clock nor the holder's
//! enters into it, so clock skew cannot expire a live lease. Each write by an
//! instance records a renewal time later than its previous write, so no two of
//! its writes have the same content and every one changes the lease's version,
//! even on a store whose version is a hash of the content (an S3 `ETag`).
//!
//! Every change of holder increments the lease's generation, and a snapshot is
//! published only if no later generation has published one
//! ([`claim_publication`]). A holder that loses the lease during an upload
//! therefore cannot publish over the newer snapshot of the instance that took
//! the lease over.
//!
//! An instance is identified by [`util::instance_id::instance_id`], the identity
//! every feature that coordinates through shared state uses. It is stable across
//! restarts, so a restarted replica resumes the lease it held.
//!
//! The lease is an [`object_store_occ::lease::Lease`]; this module adds the
//! snapshot's lease duration, the publication fence and the log lines.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    ObjectStoreExt, PutPayload, PutResult, UpdateVersion, path::Path as ObjectPath,
};
use object_store_occ::lease::{Acquired, Holder, Lease, LeaseError, LeaseIo, Role};
use object_store_occ::{ConditionalWriteError, Expected};
use parking_lot::Mutex;
use util::{RetryError, retry};

use super::{
    DatasetMetadata, SNAPSHOTS_DOCS, SnapshotManager, SnapshotUploadError, format_duration,
    is_retriable_object_store_error,
};

const LEASES_DIR: &str = "leases";
/// The snapshot interval assumed for a dataset that does not create snapshots
/// on a fixed schedule: `stream_batches`, or `refresh_complete` without a
/// `refresh_check_interval`.
const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_mins(10);
/// A lease never lasts less than this, so a short snapshot interval cannot
/// make instances trade the lease over one slow request.
const MIN_LEASE_DURATION: Duration = Duration::from_secs(30);
/// A lease never lasts more than this, so a lease that claims a longer
/// duration cannot keep the other instances waiting indefinitely.
const MAX_LEASE_DURATION: Duration = object_store_occ::lease::MAX_LEASE_DURATION;
/// The dataset property, in the snapshot metadata, recording the lease
/// generation that published the dataset's latest snapshot.
const WRITER_GENERATION_PROPERTY: &str = "writer-generation";

/// A dataset's writer lease: this instance's view of the lease, and the role
/// it last reported. Clones share that state.
#[derive(Clone)]
pub(super) struct WriterLease {
    lease: Lease,
    reported: Arc<Mutex<Reported>>,
    /// Set once the instance releases the lease on shutdown, so it does not take
    /// the lease back before it exits.
    released_on_shutdown: Arc<AtomicBool>,
    /// Held while the lease is acquired, so a release on shutdown waits for an
    /// acquisition already past the shutdown check rather than being overwritten
    /// by its renewal.
    acquiring: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Default)]
struct Reported {
    /// The role last reported, so only a change of role is logged.
    role: Option<RoleKind>,
    warned_shared_identity: bool,
}

impl Default for WriterLease {
    fn default() -> Self {
        Self::new(
            Holder::new(
                util::instance_id::instance_id(),
                util::instance_id::process_id().to_string(),
            ),
            DEFAULT_SNAPSHOT_INTERVAL,
        )
    }
}

impl WriterLease {
    fn new(holder: Holder, snapshot_interval: Duration) -> Self {
        Self {
            lease: Lease::new(holder, lease_duration(snapshot_interval)),
            reported: Arc::default(),
            released_on_shutdown: Arc::default(),
            acquiring: Arc::default(),
        }
    }

    pub(super) fn with_snapshot_interval(self, snapshot_interval: Duration) -> Self {
        Self {
            lease: self.lease.with_duration(lease_duration(snapshot_interval)),
            ..self
        }
    }

    #[cfg(test)]
    pub(super) fn for_instance(identity: &str, snapshot_interval: Duration) -> Self {
        Self::new(
            Holder::new(identity, uuid::Uuid::now_v7().to_string()),
            snapshot_interval,
        )
    }
}

fn lease_duration(snapshot_interval: Duration) -> Duration {
    snapshot_interval
        .saturating_mul(2)
        .clamp(MIN_LEASE_DURATION, MAX_LEASE_DURATION)
}

/// Whether `generation` may publish the dataset's next snapshot: no later
/// generation has published one. If it may, records it as the latest.
pub(super) fn claim_publication(dataset: &mut DatasetMetadata, generation: u64) -> bool {
    if published_writer_generation(dataset).is_some_and(|published| published > generation) {
        return false;
    }
    dataset.properties.insert(
        WRITER_GENERATION_PROPERTY.to_string(),
        generation.to_string(),
    );
    true
}

fn published_writer_generation(dataset: &DatasetMetadata) -> Option<u64> {
    dataset
        .properties
        .get(WRITER_GENERATION_PROPERTY)
        .and_then(|generation| generation.parse().ok())
}

/// What the writer lease allows this instance to do about a snapshot.
pub(super) enum WriterPermit {
    /// It holds the lease, under this generation.
    Holder { generation: u64 },
    /// The store cannot write conditionally, so it creates the snapshot
    /// without a lease.
    Unleased,
    /// Another instance holds the lease, so it skips the snapshot.
    Standby,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoleKind {
    Holder,
    Standby,
    Unsupported,
}

fn role_kind(role: &Role) -> RoleKind {
    match role {
        Role::Holder { .. } => RoleKind::Holder,
        Role::Standby { .. } => RoleKind::Standby,
        Role::Unsupported => RoleKind::Unsupported,
    }
}

/// The snapshot location as the writer lease reads and writes it: with the
/// snapshot store's retries of transient network errors.
struct RetryingLeaseIo<'a>(&'a SnapshotManager);

#[async_trait]
impl LeaseIo for RetryingLeaseIo<'_> {
    async fn read(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<(Bytes, UpdateVersion)>, object_store::Error> {
        retry(self.0.network_retry_strategy.clone(), || async {
            let result = match self.0.object_store.get(path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(err) if is_retriable_object_store_error(&err) => {
                    return Err(RetryError::transient(err));
                }
                Err(err) => return Err(RetryError::permanent(err)),
            };
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let bytes = result.bytes().await.map_err(RetryError::transient)?;
            Ok(Some((bytes, version)))
        })
        .await
    }

    async fn write(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        expected: &Expected,
    ) -> Result<PutResult, ConditionalWriteError> {
        self.0
            .conditional_put_with_retry(path, payload, expected)
            .await
    }
}

impl SnapshotManager {
    fn writer_lease_path(&self) -> ObjectPath {
        self.snapshots_location
            .clone()
            .join(LEASES_DIR)
            .join(format!("{}.json", self.dataset_name))
    }

    /// Takes or renews the dataset's snapshot writer lease, and says whether
    /// this instance creates the snapshot.
    pub(super) async fn hold_writer_lease(&self) -> Result<WriterPermit, SnapshotUploadError> {
        let _acquiring = self.writer_lease.acquiring.lock().await;
        if self
            .writer_lease
            .released_on_shutdown
            .load(Ordering::Acquire)
        {
            return Ok(WriterPermit::Standby);
        }
        let role = self.acquire_writer_lease().await?;
        self.report_writer_lease_role(&role);
        Ok(match role {
            Role::Holder { generation, .. } => WriterPermit::Holder { generation },
            Role::Standby { .. } => WriterPermit::Standby,
            Role::Unsupported => WriterPermit::Unleased,
        })
    }

    /// Lets the lease lapse at once, so another instance can take it over at
    /// its next snapshot. Best effort: a failure leaves the lease to lapse on
    /// its own.
    pub(super) async fn release_writer_lease(&self) {
        match self
            .writer_lease
            .lease
            .release(&RetryingLeaseIo(self), &self.writer_lease_path())
            .await
        {
            Ok(false) => {}
            Ok(true) => {
                tracing::debug!(dataset = %self.dataset_name, "Released the snapshot writer lease after a failed snapshot");
            }
            Err(error) => {
                tracing::debug!(dataset = %self.dataset_name, %error, "Could not release the snapshot writer lease after a failed snapshot; it lapses on its own");
            }
        }
    }

    /// Stops this instance creating the dataset's snapshots and releases its
    /// writer lease, so another instance takes over at its next snapshot instead
    /// of waiting for the lease to lapse. A snapshot already being created still
    /// completes; generation fencing refuses it if the new holder publishes first.
    pub async fn release_writer_lease_on_shutdown(&self) {
        self.writer_lease
            .released_on_shutdown
            .store(true, Ordering::Release);
        let _acquiring = self.writer_lease.acquiring.lock().await;
        match self
            .writer_lease
            .lease
            .release(&RetryingLeaseIo(self), &self.writer_lease_path())
            .await
        {
            Ok(false) => {}
            Ok(true) => tracing::info!(
                "Released the snapshot writer lease of dataset '{}' on shutdown, so another instance can take over its snapshots",
                self.dataset_name
            ),
            Err(error) => tracing::warn!(
                "Failed to release the snapshot writer lease of dataset '{}' on shutdown, so another instance takes over its snapshots only after the lease lapses ({}). Cause: {error}",
                self.dataset_name,
                format_duration(self.writer_lease.lease.duration()),
            ),
        }
    }

    async fn acquire_writer_lease(&self) -> Result<Role, SnapshotUploadError> {
        let path = self.writer_lease_path();
        self.writer_lease
            .lease
            .acquire(
                &RetryingLeaseIo(self),
                &path,
                &self.conflict_retry,
                |previous| self.next_writer_generation(previous),
            )
            .await
            .map_err(|err| match err {
                LeaseError::Read { path, source } | LeaseError::Write { path, source } => {
                    SnapshotUploadError::WriterLease {
                        dataset: self.dataset_name.clone(),
                        path,
                        source,
                    }
                }
                LeaseError::Serialize { path, source } => {
                    SnapshotUploadError::UploadSerializeMetadata { path, source }
                }
                LeaseError::Generation(err) => err,
            })
    }

    /// The generation after both `previous` and the latest generation that
    /// published a snapshot of the dataset, so a lease created after the old
    /// one was lost still outranks every snapshot already published.
    async fn next_writer_generation(&self, previous: u64) -> Result<u64, SnapshotUploadError> {
        let published = self
            .load_metadata()
            .await?
            .and_then(|handle| {
                handle
                    .metadata
                    .datasets
                    .get(&self.dataset_name)
                    .and_then(published_writer_generation)
            })
            .unwrap_or(0);
        Ok(object_store_occ::lease::next_generation(
            previous.max(published),
        ))
    }

    /// Logs a change of role; an unchanged role is logged at debug only.
    fn report_writer_lease_role(&self, role: &Role) {
        let (previous, shared_identity) = {
            let mut reported = self.writer_lease.reported.lock();
            let previous = reported.role.replace(role_kind(role));
            // A process that held the lease and now finds another process's
            // renewal under its identity is sharing that identity with it.
            let shared_identity = matches!(
                role,
                Role::Holder {
                    acquired: Acquired::Renewed {
                        by_other_process: true
                    },
                    ..
                }
            ) && previous == Some(RoleKind::Holder)
                && !reported.warned_shared_identity;
            if shared_identity {
                reported.warned_shared_identity = true;
            }
            (previous, shared_identity)
        };
        let dataset = &self.dataset_name;
        match role {
            Role::Holder {
                acquired:
                    Acquired::TookOver {
                        holder,
                        unrenewed_for,
                    },
                ..
            } => {
                tracing::info!(
                    "{}",
                    took_over_message(dataset, holder.as_deref(), *unrenewed_for)
                );
            }
            Role::Holder {
                acquired: Acquired::Created,
                ..
            } if previous == Some(RoleKind::Standby) => {
                tracing::info!("{}", unheld_lease_taken_message(dataset));
            }
            Role::Holder { .. } if shared_identity => {
                tracing::error!(
                    "{}",
                    util::instance_id::duplicate_instance_message(
                        self.writer_lease.lease.holder().identity(),
                        &self.snapshot_location_uri,
                    )
                );
            }
            Role::Holder { generation, .. } => {
                tracing::debug!(dataset = %dataset, generation, "Holding the snapshot writer lease");
            }
            Role::Standby {
                holder,
                lease_duration,
            } => match previous {
                Some(RoleKind::Holder) => {
                    tracing::warn!(
                        "{}",
                        lost_lease_message(dataset, holder.as_deref(), *lease_duration)
                    );
                }
                Some(RoleKind::Standby) => {
                    tracing::debug!(dataset = %dataset, holder = ?holder, "Skipping snapshot creation: another instance holds the snapshot writer lease");
                }
                None | Some(RoleKind::Unsupported) => {
                    tracing::info!(
                        "{}",
                        standby_message(dataset, holder.as_deref(), *lease_duration)
                    );
                }
            },
            Role::Unsupported if previous != Some(RoleKind::Unsupported) => {
                tracing::warn!("{}", unsupported_store_message(dataset));
            }
            Role::Unsupported => {}
        }
    }
}

fn holder_display(holder: Option<&str>) -> String {
    holder.map_or_else(
        || "another instance".to_string(),
        |holder| format!("instance '{holder}'"),
    )
}

fn standby_message(dataset: &str, holder: Option<&str>, lease_duration: Duration) -> String {
    format!(
        "Dataset '{dataset}' is not creating snapshots while {} holds its snapshot writer lease; this instance takes over if that lease goes {} without renewal. See: {SNAPSHOTS_DOCS}",
        holder_display(holder),
        format_duration(lease_duration)
    )
}

fn lost_lease_message(dataset: &str, holder: Option<&str>, lease_duration: Duration) -> String {
    format!(
        "Dataset '{dataset}' stopped creating snapshots because {} took over its snapshot writer lease; this instance creates them again if that lease goes {} without renewal. See: {SNAPSHOTS_DOCS}",
        holder_display(holder),
        format_duration(lease_duration)
    )
}

fn took_over_message(dataset: &str, holder: Option<&str>, unrenewed_for: Duration) -> String {
    format!(
        "Dataset '{dataset}' took over its snapshot writer lease from {}, which had not renewed it for {}; this instance now creates the dataset's snapshots.",
        holder_display(holder),
        format_duration(unrenewed_for)
    )
}

fn unheld_lease_taken_message(dataset: &str) -> String {
    format!(
        "Dataset '{dataset}' took its snapshot writer lease, which no instance held any more; this instance now creates the dataset's snapshots."
    )
}

fn unsupported_store_message(dataset: &str) -> String {
    format!(
        "Dataset '{dataset}' could not take its snapshot writer lease because its snapshot location does not support conditional writes, so every instance that creates this dataset's snapshots uploads its own. Use a location that supports conditional writes, such as Amazon S3, or run one snapshot writer. See: {SNAPSHOTS_DOCS}"
    )
}

/// Logged when a holder that lost the lease during an upload finds a later
/// generation already published.
pub(super) fn superseded_message(dataset: &str, snapshot: &ObjectPath) -> String {
    format!(
        "Dataset '{dataset}' did not publish the snapshot it uploaded to '{snapshot}' because another instance took over its snapshot writer lease and published a newer snapshot first. If this repeats, lengthen the dataset's snapshot interval so a snapshot finishes within twice that interval. See: {SNAPSHOTS_DOCS}"
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::snapshot::tests::build_manager_for_api_tests;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMode, PutMultipartOptions, PutOptions, memory::InMemory, path::Path,
    };
    use object_store_occ::lease::LeaseRecord;
    use util::retry_strategy::RetryBackoffBuilder;

    const INTERVAL: Duration = Duration::from_mins(1);

    fn manager(store: &Arc<dyn ObjectStore>, lease: WriterLease) -> SnapshotManager {
        let mut manager = build_manager_for_api_tests(Arc::new(InMemory::new()));
        manager.object_store = Arc::clone(store);
        manager.writer_lease = lease;
        manager.network_retry_strategy = RetryBackoffBuilder::new()
            .max_retries(Some(2))
            .base_interval(Duration::from_millis(1))
            .build();
        manager
    }

    fn instance(identity: &str) -> WriterLease {
        WriterLease::for_instance(identity, INTERVAL)
    }

    /// A lease that lapses as soon as it is written, so the next instance can
    /// take it over without waiting.
    fn lapsing(identity: &str) -> WriterLease {
        with_lease_duration(instance(identity), Duration::ZERO)
    }

    fn with_lease_duration(lease: WriterLease, duration: Duration) -> WriterLease {
        WriterLease {
            lease: lease.lease.with_duration(duration),
            ..lease
        }
    }

    fn memory() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn stored_record(store: &Arc<dyn ObjectStore>, manager: &SnapshotManager) -> LeaseRecord {
        let bytes = store
            .get(&manager.writer_lease_path())
            .await
            .expect("lease stored")
            .bytes()
            .await
            .expect("read lease");
        serde_json::from_slice(&bytes).expect("parse lease")
    }

    fn reported(manager: &SnapshotManager) -> Option<RoleKind> {
        manager.writer_lease.reported.lock().role
    }

    #[test]
    fn a_lease_lasts_twice_the_snapshot_interval_within_its_bounds() {
        assert_eq!(
            lease_duration(Duration::from_mins(5)),
            Duration::from_mins(10)
        );
        assert_eq!(lease_duration(Duration::from_secs(5)), MIN_LEASE_DURATION);
        assert_eq!(lease_duration(Duration::from_hours(13)), MAX_LEASE_DURATION);
        assert_eq!(lease_duration(Duration::MAX), MAX_LEASE_DURATION);
        assert_eq!(
            WriterLease::default().lease.duration(),
            lease_duration(DEFAULT_SNAPSHOT_INTERVAL)
        );
    }

    #[test]
    fn a_later_generation_published_first_blocks_publication() {
        let mut dataset = DatasetMetadata::default();
        assert!(claim_publication(&mut dataset, 2));
        assert_eq!(published_writer_generation(&dataset), Some(2));
        assert!(!claim_publication(&mut dataset, 1));
        assert_eq!(published_writer_generation(&dataset), Some(2));
        assert!(claim_publication(&mut dataset, 2));
        assert!(claim_publication(&mut dataset, 3));
        assert_eq!(published_writer_generation(&dataset), Some(3));
    }

    #[tokio::test]
    async fn only_the_lease_holder_creates_snapshots() {
        let store = memory();
        let a = manager(&store, instance("host-a"));
        let b = manager(&store, instance("host-b"));

        assert!(matches!(
            a.hold_writer_lease().await.expect("a takes the lease"),
            WriterPermit::Holder { generation: 1 }
        ));
        let first = stored_record(&store, &a).await;
        assert_eq!(first.holder_identity, "host-a");
        assert_eq!(first.lease_duration_ms, 120_000);

        assert!(matches!(
            b.hold_writer_lease().await.expect("b reads the lease"),
            WriterPermit::Standby
        ));
        assert_eq!(reported(&b), Some(RoleKind::Standby));

        // The holder renews on its next snapshot, keeping its generation and
        // the time it took the lease.
        assert!(matches!(
            a.hold_writer_lease().await.expect("a renews the lease"),
            WriterPermit::Holder { generation: 1 }
        ));
        let renewed = stored_record(&store, &a).await;
        assert_eq!(renewed.acquire_time_ms, first.acquire_time_ms);
        assert_ne!(renewed, first, "every write changes the lease");
        assert!(matches!(
            b.hold_writer_lease()
                .await
                .expect("b reads the lease again"),
            WriterPermit::Standby
        ));
    }

    #[tokio::test]
    async fn a_lapsed_lease_passes_to_the_next_instance_under_a_new_generation() {
        let store = memory();
        let a = manager(&store, lapsing("host-a"));
        let b = manager(&store, instance("host-b"));

        assert!(matches!(
            a.hold_writer_lease().await.expect("a takes the lease"),
            WriterPermit::Holder { generation: 1 }
        ));
        match b.acquire_writer_lease().await.expect("b takes over") {
            Role::Holder {
                generation: 2,
                acquired: Acquired::TookOver { holder, .. },
            } => assert_eq!(holder.as_deref(), Some("host-a")),
            other => panic!("expected a takeover under generation 2, got {other:?}"),
        }

        // The previous holder now finds a live lease that is not its own.
        assert!(matches!(
            a.hold_writer_lease().await.expect("a reads the lease"),
            WriterPermit::Standby
        ));
        assert_eq!(reported(&a), Some(RoleKind::Standby));
    }

    #[tokio::test]
    async fn a_lease_lapses_only_after_its_version_stays_unchanged_for_its_duration() {
        let store = memory();
        let a = manager(&store, instance("host-a"));
        let b = manager(
            &store,
            with_lease_duration(instance("host-b"), Duration::from_millis(50)),
        );
        a.hold_writer_lease().await.expect("a takes the lease");
        // Written with a 50ms lease, then observed: the observation starts the clock.
        let short = LeaseRecord {
            lease_duration_ms: 50,
            ..stored_record(&store, &a).await
        };
        store
            .put(
                &a.writer_lease_path(),
                serde_json::to_vec(&short).expect("serialize").into(),
            )
            .await
            .expect("shorten the lease");

        assert!(matches!(
            b.acquire_writer_lease()
                .await
                .expect("b observes the lease"),
            Role::Standby { .. }
        ));
        // Time itself is under test: the lease must stay unchanged for 50ms of
        // this instance's clock before it lapses.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(matches!(
            b.acquire_writer_lease().await.expect("b takes over"),
            Role::Holder {
                acquired: Acquired::TookOver { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_released_lease_passes_to_the_next_instance_at_once() {
        let store = memory();
        let a = manager(&store, instance("host-a"));
        let b = manager(&store, instance("host-b"));
        a.hold_writer_lease().await.expect("a takes the lease");
        assert!(matches!(
            b.hold_writer_lease().await.expect("b reads the lease"),
            WriterPermit::Standby
        ));

        a.release_writer_lease().await;
        assert_eq!(stored_record(&store, &a).await.lease_duration_ms, 0);
        assert!(matches!(
            b.hold_writer_lease().await.expect("b takes over"),
            WriterPermit::Holder { generation: 2 }
        ));
    }

    #[tokio::test]
    async fn a_lease_released_on_shutdown_passes_at_once_and_is_not_taken_back() {
        let store = memory();
        let a = manager(&store, instance("host-a"));
        let b = manager(&store, instance("host-b"));
        a.hold_writer_lease().await.expect("a takes the lease");

        a.release_writer_lease_on_shutdown().await;
        assert_eq!(stored_record(&store, &a).await.lease_duration_ms, 0);
        // A snapshot attempt while shutting down stands by rather than renewing
        // the lease it just released.
        assert!(matches!(
            a.hold_writer_lease().await.expect("a reads its state"),
            WriterPermit::Standby
        ));
        assert_eq!(stored_record(&store, &a).await.lease_duration_ms, 0);
        assert!(matches!(
            b.hold_writer_lease().await.expect("b takes over"),
            WriterPermit::Holder { generation: 2 }
        ));
    }

    /// Regression test: a snapshot attempt already acquiring the lease when shutdown
    /// begins must not renew it after shutdown released it.
    #[tokio::test]
    async fn a_release_on_shutdown_waits_for_an_acquisition_in_flight() {
        let store = Arc::new(TestStore::over(Arc::new(InMemory::new())));
        let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let a = Arc::new(manager(&shared, instance("host-a")));
        let b = manager(&shared, instance("host-b"));
        a.hold_writer_lease().await.expect("a takes the lease");

        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        *store.pause_next_read.lock() = Some((Arc::clone(&reached), Arc::clone(&resume)));
        let renewing = tokio::spawn({
            let a = Arc::clone(&a);
            async move { a.hold_writer_lease().await.map(|_| ()) }
        });
        reached.notified().await;

        let mut releasing = tokio::spawn({
            let a = Arc::clone(&a);
            async move { a.release_writer_lease_on_shutdown().await }
        });
        // Give a release that does not wait for the acquisition time to land first.
        let _ = tokio::time::timeout(Duration::from_millis(200), &mut releasing).await;
        resume.notify_one();
        renewing.await.expect("renewal task").expect("a renews");
        if !releasing.is_finished() {
            releasing.await.expect("release task");
        }

        assert_eq!(stored_record(&shared, &a).await.lease_duration_ms, 0);
        assert!(matches!(
            b.hold_writer_lease().await.expect("b takes over"),
            WriterPermit::Holder { generation: 2 }
        ));
    }

    #[tokio::test]
    async fn a_new_lease_outranks_every_generation_already_published() {
        let store = memory();
        let a = manager(&store, instance("host-a"));
        let mut metadata = super::super::SnapshotMetadata::empty("memory://snapshots".into(), 0);
        let mut dataset = DatasetMetadata {
            name: a.dataset_name().to_string(),
            ..DatasetMetadata::default()
        };
        assert!(claim_publication(&mut dataset, 7));
        metadata
            .datasets
            .insert(a.dataset_name().to_string(), dataset);
        store
            .put(
                &a.metadata_path(),
                serde_json::to_vec(&metadata).expect("serialize").into(),
            )
            .await
            .expect("write metadata");

        // The lease object is gone, but snapshots of generation 7 were published.
        assert!(matches!(
            a.hold_writer_lease().await.expect("a takes the lease"),
            WriterPermit::Holder { generation: 8 }
        ));
    }

    #[tokio::test]
    async fn a_restarted_replica_resumes_its_lease_and_a_shared_identity_is_reported() {
        let store = memory();
        let first_run = manager(&store, instance("host-a"));
        first_run
            .hold_writer_lease()
            .await
            .expect("first run takes the lease");

        // The same replica after a restart: same identity, new process.
        let second_run = manager(&store, instance("host-a"));
        assert_eq!(
            second_run
                .acquire_writer_lease()
                .await
                .expect("second run renews"),
            Role::Holder {
                generation: 1,
                acquired: Acquired::Renewed {
                    by_other_process: true
                },
            }
        );
        second_run.report_writer_lease_role(&Role::Holder {
            generation: 1,
            acquired: Acquired::Renewed {
                by_other_process: true,
            },
        });
        assert!(
            !second_run
                .writer_lease
                .reported
                .lock()
                .warned_shared_identity
        );

        // Were the first process still running, its next renewal would find the
        // second's: two live processes share the identity.
        first_run
            .hold_writer_lease()
            .await
            .expect("first run renews");
        assert!(
            first_run
                .writer_lease
                .reported
                .lock()
                .warned_shared_identity
        );
    }

    #[tokio::test]
    async fn racing_instances_elect_exactly_one_holder_of_a_new_lease() {
        let store: Arc<dyn ObjectStore> = Arc::new(TestStore::yielding());
        let managers: Vec<_> = (0..8)
            .map(|i| manager(&store, instance(&format!("host-{i}"))))
            .collect();

        let roles =
            futures::future::join_all(managers.iter().map(SnapshotManager::acquire_writer_lease))
                .await;

        let holder = stored_record(&store, &managers[0]).await.holder_identity;
        let mut holders = 0;
        for role in roles {
            match role.expect("lease read and written") {
                Role::Holder { acquired, .. } => {
                    holders += 1;
                    assert_eq!(acquired, Acquired::Created);
                }
                // Each instance that lost the race names the one that won it.
                Role::Standby { holder: named, .. } => {
                    assert_eq!(named.as_deref(), Some(holder.as_str()));
                }
                Role::Unsupported => panic!("the in-memory store writes conditionally"),
            }
        }
        assert_eq!(holders, 1);
    }

    #[tokio::test]
    async fn racing_instances_elect_exactly_one_taker_of_a_lapsed_lease() {
        let store: Arc<dyn ObjectStore> = Arc::new(TestStore::yielding());
        let gone = manager(&store, lapsing("host-gone"));
        gone.hold_writer_lease()
            .await
            .expect("the old holder takes the lease");

        let managers: Vec<_> = (1..=8)
            .map(|i| manager(&store, instance(&format!("host-{i}"))))
            .collect();
        let roles =
            futures::future::join_all(managers.iter().map(SnapshotManager::acquire_writer_lease))
                .await;

        let takeovers = roles
            .into_iter()
            .map(|role| role.expect("lease read and written"))
            .filter(|role| {
                matches!(
                    role,
                    Role::Holder {
                        acquired: Acquired::TookOver { .. },
                        ..
                    }
                )
            })
            .count();
        assert_eq!(takeovers, 1);
    }

    #[tokio::test]
    async fn a_write_that_landed_although_its_retry_was_refused_holds_the_lease() {
        let store: Arc<dyn ObjectStore> = Arc::new(TestStore::losing_next_response());
        let a = manager(&store, instance("host-a"));

        assert_eq!(
            a.acquire_writer_lease().await.expect("a takes the lease"),
            Role::Holder {
                generation: 1,
                acquired: Acquired::Created,
            }
        );
        assert_eq!(stored_record(&store, &a).await.holder_identity, "host-a");
    }

    #[tokio::test]
    async fn a_store_without_versions_creates_snapshots_without_a_lease() {
        let shared = Arc::new(InMemory::new());
        let versioned: Arc<dyn ObjectStore> = Arc::new(TestStore::over(Arc::clone(&shared)));
        let a = manager(&versioned, instance("host-a"));
        a.hold_writer_lease().await.expect("a takes the lease");

        // Even another instance's live lease cannot be matched without a version.
        let versionless: Arc<dyn ObjectStore> = Arc::new(TestStore {
            strip_versions: true,
            ..TestStore::over(shared)
        });
        let b = manager(&versionless, instance("host-b"));
        assert!(matches!(
            b.hold_writer_lease().await.expect("b reads the lease"),
            WriterPermit::Unleased
        ));
        assert_eq!(reported(&b), Some(RoleKind::Unsupported));
    }

    #[tokio::test]
    async fn a_store_without_conditional_updates_lets_the_instance_create_snapshots() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let store: Arc<dyn ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(dir.path()).expect("local store"),
        );
        let a = manager(&store, instance("host-a"));

        // The local file system can create the lease but cannot renew it.
        assert!(matches!(
            a.hold_writer_lease().await.expect("a creates the lease"),
            WriterPermit::Holder { .. }
        ));
        assert!(matches!(
            a.hold_writer_lease()
                .await
                .expect("a cannot renew the lease"),
            WriterPermit::Unleased
        ));
        assert_eq!(reported(&a), Some(RoleKind::Unsupported));
    }

    #[test]
    fn the_lease_is_kept_beside_the_metadata_and_named_for_the_dataset() {
        let manager = build_manager_for_api_tests(Arc::new(InMemory::new()));
        assert_eq!(
            manager.writer_lease_path().to_string(),
            format!("snapshots/leases/{}.json", manager.dataset_name())
        );
    }

    #[test]
    fn standby_message_names_the_dataset_the_holder_and_when_it_takes_over() {
        assert_eq!(
            standby_message("orders", Some("spice-1"), Duration::from_mins(2)),
            format!(
                "Dataset 'orders' is not creating snapshots while instance 'spice-1' holds its snapshot writer lease; this instance takes over if that lease goes 2m without renewal. See: {SNAPSHOTS_DOCS}"
            )
        );
        assert!(
            standby_message("orders", None, Duration::from_mins(2))
                .contains("while another instance holds its snapshot writer lease")
        );
    }

    #[test]
    fn lost_lease_message_names_the_new_holder() {
        assert_eq!(
            lost_lease_message("orders", Some("spice-2"), Duration::from_secs(30)),
            format!(
                "Dataset 'orders' stopped creating snapshots because instance 'spice-2' took over its snapshot writer lease; this instance creates them again if that lease goes 30s without renewal. See: {SNAPSHOTS_DOCS}"
            )
        );
    }

    #[test]
    fn took_over_message_names_the_previous_holder_and_how_long_it_was_silent() {
        assert_eq!(
            took_over_message("orders", Some("spice-1"), Duration::from_secs(130)),
            "Dataset 'orders' took over its snapshot writer lease from instance 'spice-1', which had not renewed it for 2m; this instance now creates the dataset's snapshots."
        );
    }

    #[test]
    fn unheld_lease_taken_message_names_the_dataset() {
        assert_eq!(
            unheld_lease_taken_message("orders"),
            "Dataset 'orders' took its snapshot writer lease, which no instance held any more; this instance now creates the dataset's snapshots."
        );
    }

    #[test]
    fn unsupported_store_message_names_the_dataset_the_impact_and_the_fix() {
        assert_eq!(
            unsupported_store_message("orders"),
            format!(
                "Dataset 'orders' could not take its snapshot writer lease because its snapshot location does not support conditional writes, so every instance that creates this dataset's snapshots uploads its own. Use a location that supports conditional writes, such as Amazon S3, or run one snapshot writer. See: {SNAPSHOTS_DOCS}"
            )
        );
    }

    #[test]
    fn superseded_message_names_the_dataset_the_upload_and_the_fix() {
        assert_eq!(
            superseded_message(
                "orders",
                &ObjectPath::from("snapshots/dataset=orders/orders_20260924T000000Z.sqlite")
            ),
            format!(
                "Dataset 'orders' did not publish the snapshot it uploaded to 'snapshots/dataset=orders/orders_20260924T000000Z.sqlite' because another instance took over its snapshot writer lease and published a newer snapshot first. If this repeats, lengthen the dataset's snapshot interval so a snapshot finishes within twice that interval. See: {SNAPSHOTS_DOCS}"
            )
        );
    }

    /// An in-memory store that can misbehave the ways the lease must survive.
    #[derive(Debug)]
    struct TestStore {
        inner: Arc<InMemory>,
        /// Yield before every read and write, so instances polled together all
        /// read the lease before any of them writes it: the interleaving of a
        /// real race.
        yield_on_io: bool,
        /// Apply the next conditional write but report it as failed, as when
        /// its response is lost.
        lose_next_response: AtomicBool,
        /// Report no `ETag` or version, as a store that cannot write
        /// conditionally.
        strip_versions: bool,
        /// Hold the next read until `resume` is notified, after notifying
        /// `reached`, to stop an acquisition between its check and its write.
        pause_next_read: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    }

    impl TestStore {
        fn over(inner: Arc<InMemory>) -> Self {
            Self {
                inner,
                yield_on_io: false,
                lose_next_response: AtomicBool::new(false),
                strip_versions: false,
                pause_next_read: Mutex::new(None),
            }
        }

        fn yielding() -> Self {
            Self {
                yield_on_io: true,
                ..Self::over(Arc::new(InMemory::new()))
            }
        }

        fn losing_next_response() -> Self {
            Self {
                lose_next_response: AtomicBool::new(true),
                ..Self::over(Arc::new(InMemory::new()))
            }
        }
    }

    impl std::fmt::Display for TestStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestStore")
        }
    }

    #[async_trait]
    impl ObjectStore for TestStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if self.yield_on_io {
                tokio::task::yield_now().await;
            }
            let conditional = !matches!(opts.mode, PutMode::Overwrite);
            let result = self.inner.put_opts(location, payload, opts).await?;
            if conditional && self.lose_next_response.swap(false, Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "TestStore",
                    source: "the response to a write that was applied was lost".into(),
                });
            }
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if self.yield_on_io {
                tokio::task::yield_now().await;
            }
            let pause = self.pause_next_read.lock().take();
            if let Some((reached, resume)) = pause {
                reached.notify_one();
                resume.notified().await;
            }
            let mut result = self.inner.get_opts(location, options).await?;
            if self.strip_versions {
                result.meta.e_tag = None;
                result.meta.version = None;
            }
            Ok(result)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}
