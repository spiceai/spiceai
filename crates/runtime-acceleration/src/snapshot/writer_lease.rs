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
//! An instance is identified by `SPICE_INSTANCE_ID`, or else its host name: the
//! identity Postgres replication slots use. It is stable across restarts, so a
//! restarted replica resumes the lease it held.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use chrono::Utc;
use object_store::{ObjectStoreExt, PutMode, PutPayload, UpdateVersion, path::Path as ObjectPath};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use util::{RetryError, retry};

use super::{
    DatasetMetadata, SNAPSHOTS_DOCS, SnapshotManager, SnapshotUploadError, format_duration,
    is_retriable_object_store_error,
};

const LEASES_DIR: &str = "leases";
const LEASE_FORMAT_VERSION: u32 = 1;
/// The snapshot interval assumed for a dataset that does not create snapshots
/// on a fixed schedule: `stream_batches`, or `refresh_complete` without a
/// `refresh_check_interval`.
const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_mins(10);
/// A lease never lasts less than this, so a short snapshot interval cannot
/// make instances trade the lease over one slow request.
const MIN_LEASE_DURATION: Duration = Duration::from_secs(30);
/// A lease never lasts more than this, so a lease that claims a longer
/// duration cannot keep the other instances waiting indefinitely.
const MAX_LEASE_DURATION: Duration = Duration::from_hours(24);
/// Writes of the lease per snapshot. A write is refused only when another
/// write changed the lease first, so this bounds a run of conflicts that
/// leave no holder.
const LEASE_WRITE_ATTEMPTS: usize = 3;
/// The dataset property, in the snapshot metadata, recording the lease
/// generation that published the dataset's latest snapshot.
const WRITER_GENERATION_PROPERTY: &str = "writer-generation";

/// This replica's identity: `SPICE_INSTANCE_ID`, else the host name.
static INSTANCE_IDENTITY: LazyLock<Arc<str>> = LazyLock::new(|| {
    std::env::var("SPICE_INSTANCE_ID")
        .ok()
        .filter(|identity| !identity.is_empty())
        .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned())
        .into()
});

/// This process. It tells the lease a replica's previous run held apart from
/// one written by another process that has the same identity.
static PROCESS_ID: LazyLock<Arc<str>> = LazyLock::new(|| uuid::Uuid::now_v7().to_string().into());

/// The lease object's contents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LeaseRecord {
    format_version: u32,
    holder_identity: String,
    holder_process: String,
    generation: u64,
    lease_duration_ms: u64,
    acquire_time_ms: i64,
    renew_time_ms: i64,
}

impl LeaseRecord {
    fn lease_duration(&self) -> Duration {
        Duration::from_millis(self.lease_duration_ms).min(MAX_LEASE_DURATION)
    }
}

/// A dataset's writer lease: who this instance is, how long its lease lasts,
/// and what it last saw and wrote. Clones share that state.
#[derive(Clone)]
pub(super) struct WriterLease {
    identity: Arc<str>,
    process: Arc<str>,
    duration: Duration,
    state: Arc<Mutex<LeaseState>>,
}

#[derive(Default)]
struct LeaseState {
    /// The role last reported, so only a change of role is logged.
    reported: Option<RoleKind>,
    /// The lease version last read, and when it was first read.
    observed: Option<(UpdateVersion, Instant)>,
    /// The lease as this instance last wrote it, so it can release it.
    written: Option<(UpdateVersion, LeaseRecord)>,
    /// The renewal time of this instance's latest write.
    last_renew_time_ms: Option<i64>,
    warned_shared_identity: bool,
}

impl Default for WriterLease {
    fn default() -> Self {
        Self::new(
            Arc::clone(&INSTANCE_IDENTITY),
            Arc::clone(&PROCESS_ID),
            DEFAULT_SNAPSHOT_INTERVAL,
        )
    }
}

impl WriterLease {
    fn new(identity: Arc<str>, process: Arc<str>, snapshot_interval: Duration) -> Self {
        Self {
            identity,
            process,
            duration: lease_duration(snapshot_interval),
            state: Arc::new(Mutex::new(LeaseState::default())),
        }
    }

    pub(super) fn with_snapshot_interval(self, snapshot_interval: Duration) -> Self {
        Self {
            duration: lease_duration(snapshot_interval),
            ..self
        }
    }

    #[cfg(test)]
    pub(super) fn for_instance(identity: &str, snapshot_interval: Duration) -> Self {
        Self::new(
            identity.into(),
            uuid::Uuid::now_v7().to_string().into(),
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

/// The lease as read from the store.
struct ObservedLease {
    /// `None` when the object is not a lease this version can read.
    record: Option<LeaseRecord>,
    version: UpdateVersion,
}

#[derive(Debug, PartialEq)]
enum Plan {
    /// No lease exists: create it.
    Create,
    /// The lease is held under this instance's identity: renew it.
    Renew {
        version: UpdateVersion,
        record: LeaseRecord,
    },
    /// The holder let the lease lapse: take it over.
    TakeOver {
        version: UpdateVersion,
        holder: Option<String>,
        unrenewed_for: Duration,
        previous_generation: u64,
    },
    /// Another instance holds a live lease.
    Standby {
        holder: Option<String>,
        lease_duration: Duration,
    },
}

/// Decides what to do with the lease as observed, given how long this
/// instance has seen its current version unchanged. An unreadable lease is
/// treated as another instance's, lasting this instance's own duration.
fn plan(observed: Option<&ObservedLease>, lease: &WriterLease, unchanged_for: Duration) -> Plan {
    let Some(observed) = observed else {
        return Plan::Create;
    };
    let record = observed.record.as_ref();
    if let Some(record) = record
        && *record.holder_identity == *lease.identity
    {
        return Plan::Renew {
            version: observed.version.clone(),
            record: record.clone(),
        };
    }
    let holder = record.map(|record| record.holder_identity.clone());
    let lease_duration = record.map_or(lease.duration, LeaseRecord::lease_duration);
    if unchanged_for >= lease_duration {
        Plan::TakeOver {
            version: observed.version.clone(),
            holder,
            unrenewed_for: unchanged_for,
            previous_generation: record.map_or(0, |record| record.generation),
        }
    } else {
        Plan::Standby {
            holder,
            lease_duration,
        }
    }
}

/// What this instance may do about the dataset's next snapshot.
#[derive(Debug, PartialEq)]
enum Role {
    /// It holds the lease, under this generation, and creates the snapshot.
    Holder { generation: u64, acquired: Acquired },
    /// Another instance holds the lease, so it skips the snapshot.
    Standby {
        holder: Option<String>,
        lease_duration: Duration,
    },
    /// The store cannot write conditionally, so it creates the snapshot
    /// without a lease.
    Unsupported,
}

/// How this instance came to hold the lease.
#[derive(Clone, Debug, PartialEq)]
enum Acquired {
    /// No lease existed.
    Created,
    /// The lease was held under this instance's identity; `by_other_process`
    /// when a process other than this one wrote it last.
    Renewed { by_other_process: bool },
    /// The holder let the lease lapse.
    TookOver {
        holder: Option<String>,
        unrenewed_for: Duration,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoleKind {
    Holder,
    Standby,
    Unsupported,
}

impl Role {
    fn kind(&self) -> RoleKind {
        match self {
            Role::Holder { .. } => RoleKind::Holder,
            Role::Standby { .. } => RoleKind::Standby,
            Role::Unsupported => RoleKind::Unsupported,
        }
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
        let Some((version, record)) = self.writer_lease.state.lock().written.take() else {
            return;
        };
        let released = LeaseRecord {
            lease_duration_ms: 0,
            renew_time_ms: self.next_renew_time_ms(),
            ..record
        };
        let path = self.writer_lease_path();
        let result = match serde_json::to_vec_pretty(&released) {
            Ok(body) => self
                .object_store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutMode::Update(version).into(),
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        match result {
            Ok(()) => {
                tracing::debug!(dataset = %self.dataset_name, "Released the snapshot writer lease after a failed snapshot");
            }
            Err(error) => {
                tracing::debug!(dataset = %self.dataset_name, %error, "Could not release the snapshot writer lease after a failed snapshot; it lapses on its own");
            }
        }
    }

    async fn acquire_writer_lease(&self) -> Result<Role, SnapshotUploadError> {
        let path = self.writer_lease_path();
        let lease = &self.writer_lease;
        // The lease as this instance last tried to write it, when the store
        // refused the write.
        let mut refused: Option<(LeaseRecord, Acquired)> = None;

        for _ in 0..LEASE_WRITE_ATTEMPTS {
            let observed = self.read_writer_lease(&path).await?;
            if let Some(observed) = &observed {
                // A refused write can still have landed: a retry after a lost
                // response is refused by the write it repeats.
                if let Some((record, acquired)) = refused.take()
                    && observed.record.as_ref() == Some(&record)
                {
                    let generation = record.generation;
                    self.writer_lease.state.lock().written =
                        Some((observed.version.clone(), record));
                    return Ok(Role::Holder {
                        generation,
                        acquired,
                    });
                }
                // Nothing to match a conditional write against.
                if observed.version.e_tag.is_none() && observed.version.version.is_none() {
                    return Ok(Role::Unsupported);
                }
            }

            let unchanged_for = self.lease_unchanged_for(observed.as_ref());
            let now_ms = self.next_renew_time_ms();
            let (mode, record, acquired) = match plan(observed.as_ref(), lease, unchanged_for) {
                Plan::Standby {
                    holder,
                    lease_duration,
                } => {
                    return Ok(Role::Standby {
                        holder,
                        lease_duration,
                    });
                }
                Plan::Create => (
                    PutMode::Create,
                    self.lease_record(self.next_writer_generation(0).await?, now_ms, now_ms),
                    Acquired::Created,
                ),
                Plan::Renew { version, record } => (
                    PutMode::Update(version),
                    self.lease_record(record.generation, record.acquire_time_ms, now_ms),
                    Acquired::Renewed {
                        by_other_process: *record.holder_process != *lease.process,
                    },
                ),
                Plan::TakeOver {
                    version,
                    holder,
                    unrenewed_for,
                    previous_generation,
                } => (
                    PutMode::Update(version),
                    self.lease_record(
                        self.next_writer_generation(previous_generation).await?,
                        now_ms,
                        now_ms,
                    ),
                    Acquired::TookOver {
                        holder,
                        unrenewed_for,
                    },
                ),
            };

            let body = serde_json::to_vec_pretty(&record).map_err(|source| {
                SnapshotUploadError::UploadSerializeMetadata {
                    path: path.to_string(),
                    source,
                }
            })?;
            match self
                .put_opts_with_retry(&path, PutPayload::from(body), mode)
                .await
            {
                Ok(result) => {
                    let generation = record.generation;
                    let version = UpdateVersion {
                        e_tag: result.e_tag,
                        version: result.version,
                    };
                    self.writer_lease.state.lock().written = Some((version, record));
                    return Ok(Role::Holder {
                        generation,
                        acquired,
                    });
                }
                // Another write changed the lease first; read it again.
                Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => refused = Some((record, acquired)),
                Err(
                    object_store::Error::NotImplemented { .. }
                    | object_store::Error::NotSupported { .. },
                ) => return Ok(Role::Unsupported),
                Err(source) => {
                    return Err(SnapshotUploadError::WriterLease {
                        dataset: self.dataset_name.clone(),
                        path: path.to_string(),
                        source,
                    });
                }
            }
        }

        // Every write met a conflicting one. Skip this snapshot and look again
        // before the next.
        Ok(Role::Standby {
            holder: None,
            lease_duration: lease.duration,
        })
    }

    fn lease_record(
        &self,
        generation: u64,
        acquire_time_ms: i64,
        renew_time_ms: i64,
    ) -> LeaseRecord {
        LeaseRecord {
            format_version: LEASE_FORMAT_VERSION,
            holder_identity: self.writer_lease.identity.to_string(),
            holder_process: self.writer_lease.process.to_string(),
            generation,
            lease_duration_ms: u64::try_from(self.writer_lease.duration.as_millis())
                .unwrap_or(u64::MAX),
            acquire_time_ms,
            renew_time_ms,
        }
    }

    /// The current time, or just after this instance's latest write if the
    /// clock has not moved past it, so each write's content differs.
    fn next_renew_time_ms(&self) -> i64 {
        let mut state = self.writer_lease.state.lock();
        let now_ms = Utc::now().timestamp_millis();
        let renew_time_ms = state
            .last_renew_time_ms
            .map_or(now_ms, |last| now_ms.max(last.saturating_add(1)));
        state.last_renew_time_ms = Some(renew_time_ms);
        renew_time_ms
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
        Ok(previous.max(published).saturating_add(1))
    }

    /// How long this instance has seen the lease's current version unchanged.
    fn lease_unchanged_for(&self, observed: Option<&ObservedLease>) -> Duration {
        let mut state = self.writer_lease.state.lock();
        let Some(observed) = observed else {
            state.observed = None;
            return Duration::ZERO;
        };
        match &state.observed {
            Some((version, since)) if *version == observed.version => since.elapsed(),
            _ => {
                state.observed = Some((observed.version.clone(), Instant::now()));
                Duration::ZERO
            }
        }
    }

    async fn read_writer_lease(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<ObservedLease>, SnapshotUploadError> {
        retry(self.network_retry_strategy.clone(), || async {
            let result = match self.object_store.get(path).await {
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
            let record = serde_json::from_slice::<LeaseRecord>(&bytes)
                .ok()
                .filter(|record| record.format_version == LEASE_FORMAT_VERSION);
            Ok(Some(ObservedLease { record, version }))
        })
        .await
        .map_err(|source| SnapshotUploadError::WriterLease {
            dataset: self.dataset_name.clone(),
            path: path.to_string(),
            source,
        })
    }

    /// Logs a change of role; an unchanged role is logged at debug only.
    fn report_writer_lease_role(&self, role: &Role) {
        let (previous, shared_identity) = {
            let mut state = self.writer_lease.state.lock();
            let previous = state.reported.replace(role.kind());
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
                && !state.warned_shared_identity;
            if shared_identity {
                state.warned_shared_identity = true;
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
                tracing::warn!(
                    "{}",
                    shared_identity_message(dataset, &self.writer_lease.identity)
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

fn shared_identity_message(dataset: &str, identity: &str) -> String {
    format!(
        "Dataset '{dataset}' found its snapshot writer lease renewed by another process with the same instance identity '{identity}', so both processes create the dataset's snapshots. Give each replica a distinct `SPICE_INSTANCE_ID`. See: {SNAPSHOTS_DOCS}"
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
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutResult, memory::InMemory, path::Path,
    };
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
        WriterLease {
            duration: Duration::ZERO,
            ..instance(identity)
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
        manager.writer_lease.state.lock().reported
    }

    fn record(holder: &str, generation: u64, lease_duration: Duration) -> LeaseRecord {
        LeaseRecord {
            format_version: LEASE_FORMAT_VERSION,
            holder_identity: holder.to_string(),
            holder_process: format!("{holder}-process"),
            generation,
            lease_duration_ms: u64::try_from(lease_duration.as_millis()).expect("fits"),
            acquire_time_ms: 1_000,
            renew_time_ms: 2_000,
        }
    }

    fn observed(record: Option<LeaseRecord>) -> ObservedLease {
        ObservedLease {
            record,
            version: version("7"),
        }
    }

    fn version(e_tag: &str) -> UpdateVersion {
        UpdateVersion {
            e_tag: Some(e_tag.to_string()),
            version: None,
        }
    }

    #[test]
    fn an_absent_lease_is_created() {
        assert_eq!(plan(None, &instance("a"), Duration::ZERO), Plan::Create);
    }

    #[test]
    fn a_lease_held_under_our_identity_is_renewed_even_after_a_restart() {
        // Renewed however long it went unrenewed: no other instance took it over.
        let ours = record("a", 4, Duration::from_secs(30));
        assert_eq!(
            plan(
                Some(&observed(Some(ours.clone()))),
                &instance("a"),
                Duration::from_hours(3)
            ),
            Plan::Renew {
                version: version("7"),
                record: ours,
            }
        );
    }

    #[test]
    fn a_live_lease_of_another_instance_is_left_to_it() {
        let theirs = observed(Some(record("b", 1, Duration::from_mins(2))));
        assert_eq!(
            plan(Some(&theirs), &instance("a"), Duration::from_secs(119)),
            Plan::Standby {
                holder: Some("b".to_string()),
                lease_duration: Duration::from_mins(2),
            }
        );
    }

    #[test]
    fn a_lease_unchanged_for_its_duration_is_taken_over() {
        let theirs = observed(Some(record("b", 3, Duration::from_mins(2))));
        assert_eq!(
            plan(Some(&theirs), &instance("a"), Duration::from_mins(2)),
            Plan::TakeOver {
                version: version("7"),
                holder: Some("b".to_string()),
                unrenewed_for: Duration::from_mins(2),
                previous_generation: 3,
            }
        );
    }

    #[test]
    fn the_holders_own_duration_decides_when_its_lease_lapses() {
        // Five minutes unchanged is past this instance's 30s lease, but within
        // the 10m the holder said it renews in.
        let theirs = observed(Some(record("b", 1, Duration::from_mins(10))));
        let short = WriterLease {
            duration: Duration::from_secs(30),
            ..instance("a")
        };
        assert!(matches!(
            plan(Some(&theirs), &short, Duration::from_mins(5)),
            Plan::Standby { .. }
        ));
    }

    #[test]
    fn a_lease_claiming_more_than_the_maximum_duration_lapses_at_the_maximum() {
        let theirs = observed(Some(LeaseRecord {
            lease_duration_ms: u64::MAX,
            ..record("b", 1, Duration::ZERO)
        }));
        assert!(matches!(
            plan(Some(&theirs), &instance("a"), MAX_LEASE_DURATION),
            Plan::TakeOver { .. }
        ));
    }

    #[test]
    fn an_unreadable_lease_lapses_after_our_own_duration() {
        let lease = instance("a");
        assert_eq!(
            plan(
                Some(&observed(None)),
                &lease,
                lease.duration.saturating_sub(Duration::from_secs(1))
            ),
            Plan::Standby {
                holder: None,
                lease_duration: lease.duration,
            }
        );
        assert!(matches!(
            plan(Some(&observed(None)), &lease, lease.duration),
            Plan::TakeOver {
                holder: None,
                previous_generation: 0,
                ..
            }
        ));
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
            WriterLease::default().duration,
            lease_duration(DEFAULT_SNAPSHOT_INTERVAL)
        );
    }

    #[test]
    fn the_instance_identity_is_the_host_name_without_spice_instance_id() {
        if std::env::var("SPICE_INSTANCE_ID").is_ok_and(|identity| !identity.is_empty()) {
            return;
        }
        assert_eq!(
            &**INSTANCE_IDENTITY,
            &*gethostname::gethostname().to_string_lossy()
        );
        uuid::Uuid::parse_str(&PROCESS_ID).expect("the process id is a UUID");
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

    #[test]
    fn each_write_is_renewed_later_than_the_previous_even_within_a_millisecond() {
        let a = manager(&memory(), instance("host-a"));
        // A previous write stamped ahead of this clock: the next one still
        // comes after it, so the two writes differ.
        let ahead = Utc::now().timestamp_millis() + 60_000;
        a.writer_lease.state.lock().last_renew_time_ms = Some(ahead);
        assert_eq!(a.next_renew_time_ms(), ahead + 1);
        assert_eq!(a.next_renew_time_ms(), ahead + 2);
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
            WriterLease {
                duration: Duration::from_millis(50),
                ..instance("host-b")
            },
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
        assert!(!second_run.writer_lease.state.lock().warned_shared_identity);

        // Were the first process still running, its next renewal would find the
        // second's: two live processes share the identity.
        first_run
            .hold_writer_lease()
            .await
            .expect("first run renews");
        assert!(first_run.writer_lease.state.lock().warned_shared_identity);
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
    fn shared_identity_message_names_the_identity_and_the_fix() {
        assert_eq!(
            shared_identity_message("orders", "spice-1"),
            format!(
                "Dataset 'orders' found its snapshot writer lease renewed by another process with the same instance identity 'spice-1', so both processes create the dataset's snapshots. Give each replica a distinct `SPICE_INSTANCE_ID`. See: {SNAPSHOTS_DOCS}"
            )
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
    }

    impl TestStore {
        fn over(inner: Arc<InMemory>) -> Self {
            Self {
                inner,
                yield_on_io: false,
                lose_next_response: AtomicBool::new(false),
                strip_versions: false,
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
