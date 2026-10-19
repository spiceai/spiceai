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

//! Ownership of state that instances share through object storage: which
//! process holds a piece of state, when that holder counts as gone, and the
//! generation that stops a former holder from writing after another took over.
//!
//! - A [`Holder`] is an instance identity, stable across restarts, and a
//!   per-process id that tells two runs of one instance apart.
//! - A lease object ([`LeaseRecord`]) names its holder, its generation and how
//!   long it lasts without renewal. [`Lease::acquire`] creates it with
//!   `If-None-Match: *`, and renews or takes it over with `If-Match` on the
//!   version it read, so exactly one write wins each race.
//! - A record lapses once one version of it has stayed unchanged, on the
//!   observing process's own monotonic clock, for the duration its holder gave
//!   it ([`LapseClock`]). That time runs from when the process first received
//!   the version to the start of its latest read that returned it, so a slow
//!   read is no evidence that the version stayed unchanged. No two machines'
//!   clocks are compared, so clock skew
//!   cannot expire a live holder. A process that has just started has seen no
//!   version yet, so it judges no holder gone before it has watched one
//!   version for that duration.
//! - A holder's writes carry renewal times from a [`RenewClock`], each later
//!   than its previous write, so every renewal changes the object's version,
//!   even on a store whose version is a hash of the content.
//! - Every change of holder increments the generation ([`next_generation`]).
//!   A holder records the generation it holds and checks it inside its own
//!   conditional writes of the state it guards. [`fenced_put`] does this for
//!   an object outside the record: it reads the object's version, checks that
//!   the writer still holds the state, then writes on that version, so a
//!   newer holder's write after the check makes it conflict rather than be
//!   overwritten.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::UpdateVersion;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutPayload, PutResult};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::conditional::{ConditionalWriteError, Expected, conditional_put};
use crate::retry::{Attempt, ConflictRetry, RetryOnConflictError, retry_on_conflict};

/// The format version of [`LeaseRecord`]. A record of another version reads as
/// unreadable.
pub const LEASE_FORMAT_VERSION: u32 = 1;

/// A lease never lasts more than this, so a record that claims a longer
/// duration cannot keep other instances waiting indefinitely.
pub const MAX_LEASE_DURATION: Duration = Duration::from_hours(24);

/// The generation that follows `previous`: every change of holder takes the
/// next one.
#[must_use]
pub fn next_generation(previous: u64) -> u64 {
    previous.saturating_add(1)
}

/// Who holds a piece of shared state: the instance identity and the process.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Holder {
    identity: Arc<str>,
    process: Arc<str>,
}

impl Holder {
    #[must_use]
    pub fn new(identity: impl Into<Arc<str>>, process: impl Into<Arc<str>>) -> Self {
        Self {
            identity: identity.into(),
            process: process.into(),
        }
    }

    /// The instance identity, stable across restarts.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The process, unique to one run of the instance.
    #[must_use]
    pub fn process(&self) -> &str {
        &self.process
    }
}

/// A lease object's contents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LeaseRecord {
    pub format_version: u32,
    pub holder_identity: String,
    pub holder_process: String,
    pub generation: u64,
    pub lease_duration_ms: u64,
    pub acquire_time_ms: i64,
    pub renew_time_ms: i64,
    /// What the holder reports about the state it guards, for instances that list it.
    /// Left out when the holder reports nothing; a version of Spice that does not know
    /// the field ignores it, so it needs no new format version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<serde_json::Value>,
}

impl LeaseRecord {
    /// How long the lease lasts without renewal, at most
    /// [`MAX_LEASE_DURATION`].
    #[must_use]
    pub fn lease_duration(&self) -> Duration {
        Duration::from_millis(self.lease_duration_ms).min(MAX_LEASE_DURATION)
    }

    /// Parses a stored record. `None` when it is not a lease record of
    /// [`LEASE_FORMAT_VERSION`].
    #[must_use]
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice::<Self>(bytes)
            .ok()
            .filter(|record| record.format_version == LEASE_FORMAT_VERSION)
    }
}

/// A version of an object and when this process first received it.
#[derive(Clone, Debug)]
struct Seen {
    version: Option<UpdateVersion>,
    since: Instant,
}

impl Seen {
    /// Records a read of `version` (`None`: the object was absent) that started
    /// at `read_at`, and returns how long this process has seen that version
    /// unchanged.
    fn observe(
        seen: &mut Option<Self>,
        version: Option<&UpdateVersion>,
        read_at: Instant,
    ) -> Duration {
        match seen {
            Some(current) if current.version.as_ref() == version => {
                read_at.saturating_duration_since(current.since)
            }
            _ => {
                *seen = Some(Self {
                    version: version.cloned(),
                    since: Instant::now(),
                });
                Duration::ZERO
            }
        }
    }
}

/// The lapse rule for many objects at once: how long this process has seen
/// each object's version unchanged, on its own monotonic clock.
#[derive(Debug, Default)]
pub struct LapseClock {
    seen: Mutex<HashMap<String, Option<Seen>>>,
}

impl LapseClock {
    /// Records a read of the object `key` at `version` (`None`: the object was
    /// absent) and returns how long this process has seen that version
    /// unchanged. `read_at` is when the read started, which bounds what it
    /// shows. Zero the first time a version is seen.
    pub fn observe(
        &self,
        key: &str,
        version: Option<&UpdateVersion>,
        read_at: Instant,
    ) -> Duration {
        let mut seen = self.seen.lock();
        Seen::observe(seen.entry(key.to_string()).or_default(), version, read_at)
    }

    /// Whether the object `key`, read at `version` by a read that started at
    /// `read_at`, has stayed unchanged for `duration`. Records the read like
    /// [`Self::observe`].
    pub fn lapsed(
        &self,
        key: &str,
        version: Option<&UpdateVersion>,
        read_at: Instant,
        duration: Duration,
    ) -> bool {
        self.observe(key, version, read_at) >= duration
    }

    /// Forgets every object whose key `keep` rejects.
    pub fn retain(&self, mut keep: impl FnMut(&str) -> bool) {
        self.seen.lock().retain(|key, _| keep(key));
    }
}

/// Renewal times for one holder's writes: the wall clock, or just after the
/// previous renewal if the clock has not moved past it. Two writes of one
/// holder therefore never carry the same time.
#[derive(Debug, Default)]
pub struct RenewClock {
    last_ms: Mutex<Option<i64>>,
}

impl RenewClock {
    /// The renewal time of the next write, in milliseconds since the Unix epoch.
    pub fn next_ms(&self) -> i64 {
        self.next_after(wall_clock_ms())
    }

    fn next_after(&self, now_ms: i64) -> i64 {
        let mut last = self.last_ms.lock();
        let next = last.map_or(now_ms, |last| now_ms.max(last.saturating_add(1)));
        *last = Some(next);
        next
    }
}

fn wall_clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// A lease as read from the store.
#[derive(Clone, Debug)]
pub struct ObservedLease {
    /// `None` when the object is not a lease record this version can read.
    pub record: Option<LeaseRecord>,
    pub version: UpdateVersion,
}

/// What a holder does with the lease it read.
#[derive(Debug, PartialEq)]
pub enum Plan {
    /// No lease exists: create it.
    Create,
    /// The lease is held under this holder's identity: renew it.
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

/// Decides what `holder`, whose own leases last `duration`, does with the
/// lease as observed, given how long it has seen the lease's current version
/// unchanged.
///
/// A lease held under the holder's identity is renewed whichever process wrote
/// it: a restarted instance resumes the lease its previous run held. A lease
/// lapses after the duration its holder wrote into it; an unreadable one is
/// treated as another instance's, lasting `duration`.
#[must_use]
pub fn plan(
    observed: Option<&ObservedLease>,
    holder: &Holder,
    duration: Duration,
    unchanged_for: Duration,
) -> Plan {
    let Some(observed) = observed else {
        return Plan::Create;
    };
    let record = observed.record.as_ref();
    if let Some(record) = record
        && record.holder_identity == holder.identity()
    {
        return Plan::Renew {
            version: observed.version.clone(),
            record: record.clone(),
        };
    }
    let previous_holder = record.map(|record| record.holder_identity.clone());
    let lease_duration = record.map_or(duration, LeaseRecord::lease_duration);
    if unchanged_for >= lease_duration {
        Plan::TakeOver {
            version: observed.version.clone(),
            holder: previous_holder,
            unrenewed_for: unchanged_for,
            previous_generation: record.map_or(0, |record| record.generation),
        }
    } else {
        Plan::Standby {
            holder: previous_holder,
            lease_duration,
        }
    }
}

/// What [`Lease::acquire`] found this holder may do.
#[derive(Debug, PartialEq)]
pub enum Role {
    /// It holds the lease, under this generation.
    Holder { generation: u64, acquired: Acquired },
    /// Another instance holds a live lease.
    Standby {
        holder: Option<String>,
        lease_duration: Duration,
    },
    /// The store cannot write conditionally, so no lease can be held.
    Unsupported,
}

/// How a holder came to hold the lease.
#[derive(Clone, Debug, PartialEq)]
pub enum Acquired {
    /// No lease existed.
    Created,
    /// The lease was held under this holder's identity; `by_other_process`
    /// when a process other than this one wrote it last.
    Renewed { by_other_process: bool },
    /// The previous holder let the lease lapse.
    TookOver {
        holder: Option<String>,
        unrenewed_for: Duration,
    },
}

/// Why [`Lease::acquire`] or [`Lease::release`] failed.
#[derive(Debug)]
pub enum LeaseError<E> {
    /// Reading the lease failed.
    Read {
        path: String,
        source: ObjectStoreError,
    },
    /// Writing the lease failed for a reason other than a conflict.
    Write {
        path: String,
        source: ObjectStoreError,
    },
    /// The record could not be serialized.
    Serialize {
        path: String,
        source: serde_json::Error,
    },
    /// The caller's generation lookup failed.
    Generation(E),
}

impl<E: std::fmt::Display> std::fmt::Display for LeaseError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(f, "Failed to read the lease at {path}: {source}")
            }
            Self::Write { path, source } => {
                write!(f, "Failed to write the lease at {path}: {source}")
            }
            Self::Serialize { path, source } => {
                write!(f, "Failed to serialize the lease at {path}: {source}")
            }
            Self::Generation(err) => write!(f, "{err}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for LeaseError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read { source, .. } | Self::Write { source, .. } => Some(source),
            Self::Serialize { source, .. } => Some(source),
            Self::Generation(err) => Some(err),
        }
    }
}

/// How [`Lease`] reads and writes its object. Implemented for
/// `dyn ObjectStore` with single attempts; a caller can wrap its own retries.
#[async_trait]
pub trait LeaseIo: Send + Sync {
    /// Reads the object: its bytes and version, or `None` when it does not
    /// exist.
    async fn read(&self, path: &Path) -> Result<Option<(Bytes, UpdateVersion)>, ObjectStoreError>;

    /// Writes `payload` only if the object is in the `expected` state.
    async fn write(
        &self,
        path: &Path,
        payload: PutPayload,
        expected: &Expected,
    ) -> Result<PutResult, ConditionalWriteError>;
}

#[async_trait]
impl LeaseIo for dyn ObjectStore {
    async fn read(&self, path: &Path) -> Result<Option<(Bytes, UpdateVersion)>, ObjectStoreError> {
        let result = match self.get(path).await {
            Ok(result) => result,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(err),
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        Ok(Some((result.bytes().await?, version)))
    }

    async fn write(
        &self,
        path: &Path,
        payload: PutPayload,
        expected: &Expected,
    ) -> Result<PutResult, ConditionalWriteError> {
        conditional_put(self, path, payload, expected).await
    }
}

/// One holder's view of one lease object: who it is, how long its leases
/// last, and what it last saw and wrote. Clones share that state; the object
/// itself is named on each call.
#[derive(Clone, Debug)]
pub struct Lease {
    holder: Holder,
    duration: Duration,
    state: Arc<Mutex<LeaseState>>,
}

#[derive(Debug, Default)]
struct LeaseState {
    /// The lease version last read, and when it was first read.
    seen: Option<Seen>,
    /// The lease as this holder last wrote it, so it can release it.
    written: Option<(UpdateVersion, LeaseRecord)>,
    renew: RenewClock,
    /// What each write of the lease reports as [`LeaseRecord::status`].
    status: Option<serde_json::Value>,
}

impl Lease {
    #[must_use]
    pub fn new(holder: Holder, duration: Duration) -> Self {
        Self {
            holder,
            duration,
            state: Arc::default(),
        }
    }

    /// The same lease with leases that last `duration`.
    #[must_use]
    pub fn with_duration(self, duration: Duration) -> Self {
        Self { duration, ..self }
    }

    /// Sets what each later write of the lease reports as [`LeaseRecord::status`].
    /// Returns whether it changed.
    #[must_use]
    pub fn set_status(&self, status: serde_json::Value) -> bool {
        let mut state = self.state.lock();
        if state.status.as_ref() == Some(&status) {
            return false;
        }
        state.status = Some(status);
        true
    }

    /// Whether this holder's last write of the lease succeeded and it has not released
    /// it since.
    #[must_use]
    pub fn last_written(&self) -> bool {
        self.state.lock().written.is_some()
    }

    #[must_use]
    pub fn holder(&self) -> &Holder {
        &self.holder
    }

    #[must_use]
    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// The renewal time of this holder's next write.
    #[must_use]
    pub fn next_renew_time_ms(&self) -> i64 {
        self.state.lock().renew.next_ms()
    }

    /// Creates, renews or takes over the lease at `path`, and says what this
    /// holder may do.
    ///
    /// `next_generation` gives the generation of a lease this holder creates
    /// or takes over from the previous holder's (`0` when there was none); it
    /// must return more than its argument. A renewal keeps its generation.
    /// When every write meets a conflicting one, the holder stands by.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError`] when a read or write fails, or when
    /// `next_generation` does.
    pub async fn acquire<I, E, F, Fut>(
        &self,
        io: &I,
        path: &Path,
        policy: &ConflictRetry,
        next_generation: F,
    ) -> Result<Role, LeaseError<E>>
    where
        I: LeaseIo + ?Sized,
        F: Fn(u64) -> Fut,
        Fut: Future<Output = Result<u64, E>>,
    {
        // The lease as this holder last tried to write it, when the store
        // refused the write.
        let refused: Mutex<Option<(LeaseRecord, Acquired)>> = Mutex::new(None);

        let role = retry_on_conflict(policy, || async {
            let read_at = Instant::now();
            let observed = self.read(io, path).await?;
            if let Some(observed) = &observed {
                // A refused write can still have landed: a retry after a lost
                // response is refused by the write it repeats.
                let last_refused = refused.lock().take();
                if let Some((record, acquired)) = last_refused
                    && observed.record.as_ref() == Some(&record)
                {
                    let generation = record.generation;
                    self.state.lock().written = Some((observed.version.clone(), record));
                    return Ok(Attempt::Done(Role::Holder {
                        generation,
                        acquired,
                    }));
                }
                // Nothing to match a conditional write against.
                if observed.version.e_tag.is_none() && observed.version.version.is_none() {
                    return Ok(Attempt::Done(Role::Unsupported));
                }
            }

            let unchanged_for = self.unchanged_for(observed.as_ref(), read_at);
            let now_ms = self.next_renew_time_ms();
            let (expected, record, acquired) = match plan(
                observed.as_ref(),
                &self.holder,
                self.duration,
                unchanged_for,
            ) {
                Plan::Standby {
                    holder,
                    lease_duration,
                } => {
                    return Ok(Attempt::Done(Role::Standby {
                        holder,
                        lease_duration,
                    }));
                }
                Plan::Create => (
                    Expected::Absent,
                    self.record(
                        next_generation(0).await.map_err(LeaseError::Generation)?,
                        now_ms,
                        now_ms,
                    ),
                    Acquired::Created,
                ),
                Plan::Renew { version, record } => (
                    Expected::Version(version),
                    self.record(record.generation, record.acquire_time_ms, now_ms),
                    Acquired::Renewed {
                        by_other_process: record.holder_process != self.holder.process(),
                    },
                ),
                Plan::TakeOver {
                    version,
                    holder,
                    unrenewed_for,
                    previous_generation,
                } => (
                    Expected::Version(version),
                    self.record(
                        next_generation(previous_generation)
                            .await
                            .map_err(LeaseError::Generation)?,
                        now_ms,
                        now_ms,
                    ),
                    Acquired::TookOver {
                        holder,
                        unrenewed_for,
                    },
                ),
            };

            let body =
                serde_json::to_vec_pretty(&record).map_err(|source| LeaseError::Serialize {
                    path: path.to_string(),
                    source,
                })?;
            match io.write(path, PutPayload::from(body), &expected).await {
                Ok(result) => {
                    let generation = record.generation;
                    self.state.lock().written = Some((UpdateVersion::from(result), record));
                    Ok(Attempt::Done(Role::Holder {
                        generation,
                        acquired,
                    }))
                }
                // Another write changed the lease first; read it again.
                Err(ConditionalWriteError::Conflict { .. }) => {
                    *refused.lock() = Some((record, acquired));
                    Ok(Attempt::Conflict)
                }
                Err(ConditionalWriteError::Unsupported { .. }) => {
                    Ok(Attempt::Done(Role::Unsupported))
                }
                Err(ConditionalWriteError::Store { source, .. }) => Err(LeaseError::Write {
                    path: path.to_string(),
                    source,
                }),
            }
        })
        .await;

        match role {
            Ok(role) => Ok(role),
            // Every write met a conflicting one. Stand by and look again later.
            Err(RetryOnConflictError::ConflictsExhausted { .. }) => Ok(Role::Standby {
                holder: None,
                lease_duration: self.duration,
            }),
            Err(RetryOnConflictError::Failed(err)) => Err(err),
        }
    }

    /// Lets the lease this holder last wrote lapse at once, so another holder
    /// can take it over at its next attempt. Returns `false` when this holder
    /// has written no lease since it last released one.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError`] when the release could not be written; the lease
    /// then lapses on its own.
    pub async fn release<I: LeaseIo + ?Sized>(
        &self,
        io: &I,
        path: &Path,
    ) -> Result<bool, LeaseError<ConditionalWriteError>> {
        let Some((version, record)) = self.state.lock().written.take() else {
            return Ok(false);
        };
        let released = LeaseRecord {
            lease_duration_ms: 0,
            renew_time_ms: self.next_renew_time_ms(),
            ..record
        };
        let body =
            serde_json::to_vec_pretty(&released).map_err(|source| LeaseError::Serialize {
                path: path.to_string(),
                source,
            })?;
        let expected = Expected::at_version(version).ok_or_else(|| LeaseError::Write {
            path: path.to_string(),
            source: ObjectStoreError::NotSupported {
                source: "the lease was written without a version".into(),
            },
        })?;
        io.write(path, PutPayload::from(body), &expected)
            .await
            .map_err(LeaseError::Generation)?;
        Ok(true)
    }

    async fn read<I: LeaseIo + ?Sized, E>(
        &self,
        io: &I,
        path: &Path,
    ) -> Result<Option<ObservedLease>, LeaseError<E>> {
        let read = io.read(path).await.map_err(|source| LeaseError::Read {
            path: path.to_string(),
            source,
        })?;
        Ok(read.map(|(bytes, version)| ObservedLease {
            record: LeaseRecord::parse(&bytes),
            version,
        }))
    }

    fn record(&self, generation: u64, acquire_time_ms: i64, renew_time_ms: i64) -> LeaseRecord {
        LeaseRecord {
            format_version: LEASE_FORMAT_VERSION,
            holder_identity: self.holder.identity().to_string(),
            holder_process: self.holder.process().to_string(),
            generation,
            lease_duration_ms: u64::try_from(self.duration.as_millis()).unwrap_or(u64::MAX),
            acquire_time_ms,
            renew_time_ms,
            status: self.state.lock().status.clone(),
        }
    }

    /// How long this holder has seen the lease's current version unchanged, as
    /// of a read that started at `read_at`.
    fn unchanged_for(&self, observed: Option<&ObservedLease>, read_at: Instant) -> Duration {
        let mut state = self.state.lock();
        if let Some(observed) = observed {
            Seen::observe(&mut state.seen, Some(&observed.version), read_at)
        } else {
            state.seen = None;
            Duration::ZERO
        }
    }
}

/// What [`fenced_put`] did.
#[derive(Debug)]
pub enum Fenced {
    /// The write landed while the writer held the state.
    Written(PutResult),
    /// The writer no longer holds the state, so nothing was written.
    Refused,
}

/// Why [`fenced_put`] failed.
#[derive(Debug)]
pub enum FencedPutError<E> {
    /// The holder check failed.
    Check(E),
    /// Reading the object's version or writing it failed.
    Store(ConditionalWriteError),
    /// Another writer changed the object before each of `attempts` attempts.
    Contention { path: String, attempts: usize },
}

impl<E: std::fmt::Display> std::fmt::Display for FencedPutError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Check(err) => write!(f, "{err}"),
            Self::Store(err) => write!(f, "{err}"),
            Self::Contention { path, attempts } => write!(
                f,
                "Another writer changed '{path}' before each of {attempts} attempts"
            ),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for FencedPutError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Check(err) => Some(err),
            Self::Store(err) => Some(err),
            Self::Contention { .. } => None,
        }
    }
}

/// Writes `payload` to `path` only while `holds` confirms that the writer
/// still holds the state the object belongs to, over whatever the object
/// holds now.
///
/// Each attempt reads the object's version, then calls `holds`, then writes
/// conditionally on that version. A holder that took over after the check
/// writes after the read, so this write then conflicts instead of overwriting
/// it, and the next attempt's check refuses.
///
/// # Errors
///
/// Returns [`FencedPutError`] when `holds` fails, the store fails, or every
/// attempt lost to another writer.
pub async fn fenced_put<E, F, Fut>(
    store: &dyn ObjectStore,
    path: &Path,
    payload: PutPayload,
    policy: &ConflictRetry,
    holds: F,
) -> Result<Fenced, FencedPutError<E>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<bool, E>>,
{
    let written = retry_on_conflict(policy, || async {
        let expected = match store.head(path).await {
            Ok(meta) => Expected::at_version(UpdateVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            })
            .ok_or_else(|| {
                FencedPutError::Store(ConditionalWriteError::Unsupported {
                    path: path.to_string(),
                    source: ObjectStoreError::NotSupported {
                        source: "the store reported neither an ETag nor a version".into(),
                    },
                })
            })?,
            Err(ObjectStoreError::NotFound { .. }) => Expected::Absent,
            Err(source) => {
                return Err(FencedPutError::Store(ConditionalWriteError::Store {
                    path: path.to_string(),
                    source,
                }));
            }
        };
        if !holds().await.map_err(FencedPutError::Check)? {
            return Ok(Attempt::Done(Fenced::Refused));
        }
        match conditional_put(store, path, payload.clone(), &expected).await {
            Ok(result) => Ok(Attempt::Done(Fenced::Written(result))),
            Err(err) if err.is_conflict() => Ok(Attempt::Conflict),
            Err(err) => Err(FencedPutError::Store(err)),
        }
    })
    .await;
    match written {
        Ok(fenced) => Ok(fenced),
        Err(RetryOnConflictError::ConflictsExhausted { attempts }) => {
            Err(FencedPutError::Contention {
                path: path.to_string(),
                attempts,
            })
        }
        Err(RetryOnConflictError::Failed(err)) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use super::*;
    use object_store::memory::InMemory;

    fn holder(identity: &str) -> Holder {
        Holder::new(identity, format!("{identity}-process"))
    }

    fn memory() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn path() -> Path {
        Path::from("leases/orders.json")
    }

    fn fast() -> ConflictRetry {
        ConflictRetry {
            max_attempts: 8,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(4),
        }
    }

    /// A generation lookup with nothing beyond the previous holder's.
    async fn after(previous: u64) -> Result<u64, Infallible> {
        Ok(next_generation(previous))
    }

    async fn acquire(lease: &Lease, store: &Arc<dyn ObjectStore>) -> Role {
        lease
            .acquire(store.as_ref(), &path(), &fast(), after)
            .await
            .expect("lease read and written")
    }

    async fn stored(store: &Arc<dyn ObjectStore>) -> LeaseRecord {
        let bytes = store
            .get(&path())
            .await
            .expect("lease stored")
            .bytes()
            .await
            .expect("read lease");
        LeaseRecord::parse(&bytes).expect("a lease record")
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
            status: None,
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
        assert_eq!(
            plan(None, &holder("a"), Duration::from_secs(30), Duration::ZERO),
            Plan::Create
        );
    }

    #[test]
    fn a_lease_held_under_our_identity_is_renewed_even_after_a_restart() {
        // Renewed however long it went unrenewed: no other instance took it over.
        let ours = record("a", 4, Duration::from_secs(30));
        assert_eq!(
            plan(
                Some(&observed(Some(ours.clone()))),
                &Holder::new("a", "a-next-run"),
                Duration::from_secs(30),
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
            plan(
                Some(&theirs),
                &holder("a"),
                Duration::from_secs(30),
                Duration::from_secs(119)
            ),
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
            plan(
                Some(&theirs),
                &holder("a"),
                Duration::from_secs(30),
                Duration::from_mins(2)
            ),
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
        // Five minutes unchanged is past this holder's 30s lease, but within
        // the 10m the holder said it renews in.
        let theirs = observed(Some(record("b", 1, Duration::from_mins(10))));
        assert!(matches!(
            plan(
                Some(&theirs),
                &holder("a"),
                Duration::from_secs(30),
                Duration::from_mins(5)
            ),
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
            plan(
                Some(&theirs),
                &holder("a"),
                Duration::from_secs(30),
                MAX_LEASE_DURATION
            ),
            Plan::TakeOver { .. }
        ));
    }

    #[test]
    fn an_unreadable_lease_lapses_after_our_own_duration() {
        let duration = Duration::from_secs(30);
        assert_eq!(
            plan(
                Some(&observed(None)),
                &holder("a"),
                duration,
                duration.saturating_sub(Duration::from_secs(1))
            ),
            Plan::Standby {
                holder: None,
                lease_duration: duration,
            }
        );
        assert!(matches!(
            plan(Some(&observed(None)), &holder("a"), duration, duration),
            Plan::TakeOver {
                holder: None,
                previous_generation: 0,
                ..
            }
        ));
    }

    #[test]
    fn a_record_of_another_format_version_is_unreadable() {
        let other = LeaseRecord {
            format_version: LEASE_FORMAT_VERSION + 1,
            ..record("b", 1, Duration::from_secs(30))
        };
        let bytes = serde_json::to_vec(&other).expect("serialize");
        assert_eq!(LeaseRecord::parse(&bytes), None);
        assert_eq!(LeaseRecord::parse(b"not a lease"), None);
    }

    #[test]
    fn each_renewal_is_later_than_the_previous_even_when_the_clock_is_not() {
        let clock = RenewClock::default();
        assert_eq!(clock.next_after(5_000), 5_000);
        // The clock stepped back, or did not move: the next renewal still
        // comes after the previous one.
        assert_eq!(clock.next_after(4_000), 5_001);
        assert_eq!(clock.next_after(5_001), 5_002);
        assert_eq!(clock.next_after(9_000), 9_000);
    }

    #[tokio::test]
    async fn create_then_renew_keeps_the_generation_and_changes_the_version() {
        let store = memory();
        let a = Lease::new(holder("a"), Duration::from_secs(30));

        assert_eq!(
            acquire(&a, &store).await,
            Role::Holder {
                generation: 1,
                acquired: Acquired::Created,
            }
        );
        let created = stored(&store).await;
        let created_version = store.head(&path()).await.expect("head").e_tag;
        assert_eq!(created.holder_identity, "a");
        assert_eq!(created.holder_process, "a-process");
        assert_eq!(created.lease_duration_ms, 30_000);

        assert_eq!(
            acquire(&a, &store).await,
            Role::Holder {
                generation: 1,
                acquired: Acquired::Renewed {
                    by_other_process: false
                },
            }
        );
        let renewed = stored(&store).await;
        assert_eq!(renewed.acquire_time_ms, created.acquire_time_ms);
        assert!(renewed.renew_time_ms > created.renew_time_ms);
        assert_ne!(
            store.head(&path()).await.expect("head").e_tag,
            created_version,
            "every renewal changes the lease's version"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn another_holder_takes_over_only_after_the_lease_stays_unchanged_for_its_duration() {
        let store = memory();
        let a = Lease::new(holder("a"), Duration::from_secs(30));
        let b = Lease::new(holder("b"), Duration::from_secs(30));
        acquire(&a, &store).await;

        // b's first read starts its clock; a stops renewing.
        assert_eq!(
            acquire(&b, &store).await,
            Role::Standby {
                holder: Some("a".to_string()),
                lease_duration: Duration::from_secs(30),
            }
        );
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(
            matches!(acquire(&b, &store).await, Role::Standby { .. }),
            "29s unchanged is not a lapse of a 30s lease"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        match acquire(&b, &store).await {
            Role::Holder {
                generation: 2,
                acquired:
                    Acquired::TookOver {
                        holder,
                        unrenewed_for,
                    },
            } => {
                assert_eq!(holder.as_deref(), Some("a"));
                assert_eq!(unrenewed_for, Duration::from_secs(30));
            }
            other => panic!("expected a takeover under generation 2, got {other:?}"),
        }
        assert_eq!(stored(&store).await.holder_identity, "b");

        // The former holder finds a live lease that is not its own.
        assert!(matches!(
            acquire(&a, &store).await,
            Role::Standby { holder: Some(h), .. } if h == "b"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_renewing_holder_is_never_taken_over() {
        let store = memory();
        let a = Lease::new(holder("a"), Duration::from_secs(30));
        let b = Lease::new(holder("b"), Duration::from_secs(30));
        acquire(&a, &store).await;
        for _ in 0..12 {
            assert!(matches!(acquire(&b, &store).await, Role::Standby { .. }));
            tokio::time::advance(Duration::from_secs(10)).await;
            acquire(&a, &store).await;
        }
        assert!(matches!(acquire(&b, &store).await, Role::Standby { .. }));
    }

    /// The lapse rule reads no clock but the observer's: a holder whose clock
    /// is a day ahead or a day behind stays live while it renews, and lapses
    /// on the observer's clock once it stops.
    #[tokio::test(start_paused = true)]
    async fn a_holder_whose_clock_is_far_ahead_or_behind_is_judged_by_the_observers_clock() {
        for skew_ms in [86_400_000_i64, -86_400_000] {
            let store = memory();
            let skewed = RenewClock::default();
            let write = |generation: u64| {
                let now = skewed.next_after(wall_clock_ms().saturating_add(skew_ms));
                let record = LeaseRecord {
                    acquire_time_ms: now,
                    renew_time_ms: now,
                    status: None,
                    ..record("skewed", generation, Duration::from_secs(30))
                };
                serde_json::to_vec(&record).expect("serialize")
            };
            let observer = Lease::new(holder("observer"), Duration::from_secs(30));

            store
                .put(&path(), write(1).into())
                .await
                .expect("the skewed holder writes");
            for _ in 0..6 {
                assert!(
                    matches!(acquire(&observer, &store).await, Role::Standby { .. }),
                    "a renewing holder with a clock skewed by {skew_ms}ms is live"
                );
                tokio::time::advance(Duration::from_secs(10)).await;
                store
                    .put(&path(), write(1).into())
                    .await
                    .expect("the skewed holder renews");
            }
            assert!(matches!(
                acquire(&observer, &store).await,
                Role::Standby { .. }
            ));
            tokio::time::advance(Duration::from_secs(30)).await;
            assert!(
                matches!(
                    acquire(&observer, &store).await,
                    Role::Holder {
                        generation: 2,
                        acquired: Acquired::TookOver { .. },
                    }
                ),
                "the holder with a clock skewed by {skew_ms}ms lapses 30s after its last renewal"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_observer_judges_no_holder_gone_before_watching_it_for_the_duration() {
        let clock = LapseClock::default();
        let v1 = version("1");
        // However long ago the holder wrote this version, a first read is no lapse.
        assert!(!clock.lapsed("a", Some(&v1), Instant::now(), Duration::from_secs(30)));
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(!clock.lapsed("a", Some(&v1), Instant::now(), Duration::from_secs(30)));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(clock.lapsed("a", Some(&v1), Instant::now(), Duration::from_secs(30)));
        // A new version restarts the clock; so does an object that disappears.
        assert!(!clock.lapsed(
            "a",
            Some(&version("2")),
            Instant::now(),
            Duration::from_secs(30)
        ));
        assert!(!clock.lapsed("a", None, Instant::now(), Duration::from_secs(30)));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(clock.lapsed("a", None, Instant::now(), Duration::from_secs(30)));
        clock.retain(|key| key != "a");
        assert!(!clock.lapsed("a", None, Instant::now(), Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn a_released_lease_is_taken_over_at_once() {
        let store = memory();
        let a = Lease::new(holder("a"), Duration::from_secs(30));
        let b = Lease::new(holder("b"), Duration::from_secs(30));
        acquire(&a, &store).await;
        assert!(matches!(acquire(&b, &store).await, Role::Standby { .. }));

        assert!(a.release(store.as_ref(), &path()).await.expect("release"));
        assert_eq!(stored(&store).await.lease_duration_ms, 0);
        assert!(
            !a.release(store.as_ref(), &path()).await.expect("release"),
            "nothing left to release"
        );
        assert!(matches!(
            acquire(&b, &store).await,
            Role::Holder {
                generation: 2,
                acquired: Acquired::TookOver { .. }
            }
        ));
    }

    #[tokio::test]
    async fn the_callers_generation_lookup_decides_a_new_leases_generation() {
        let store = memory();
        let a = Lease::new(holder("a"), Duration::from_secs(30));
        let role = a
            .acquire(store.as_ref(), &path(), &fast(), |previous| async move {
                Ok::<_, Infallible>(previous.max(7).saturating_add(1))
            })
            .await
            .expect("acquire");
        assert!(matches!(role, Role::Holder { generation: 8, .. }));
    }

    #[tokio::test]
    async fn racing_holders_elect_exactly_one() {
        let store: Arc<dyn ObjectStore> = Arc::new(Yielding(InMemory::new()));
        let leases: Vec<_> = (0..8)
            .map(|i| Lease::new(holder(&format!("host-{i}")), Duration::from_secs(30)))
            .collect();
        let roles =
            futures::future::join_all(leases.iter().map(|lease| acquire(lease, &store))).await;
        let winner = stored(&store).await.holder_identity;
        let mut holders = 0;
        for role in roles {
            match role {
                Role::Holder { acquired, .. } => {
                    holders += 1;
                    assert_eq!(acquired, Acquired::Created);
                }
                Role::Standby { holder, .. } => {
                    assert_eq!(holder.as_deref(), Some(winner.as_str()));
                }
                Role::Unsupported => panic!("the in-memory store writes conditionally"),
            }
        }
        assert_eq!(holders, 1);
    }

    /// A former holder is refused by the generation: its fenced write of the
    /// state it guarded does not land once another holder took over, even
    /// when it checked just before the takeover.
    #[tokio::test]
    async fn a_former_holder_is_refused_at_its_next_fenced_write() {
        let store = memory();
        let data = Path::from("state/result.arrow");
        let generation = AtomicU64::new(1);
        let holds = |mine: u64| {
            let current = generation.load(Ordering::SeqCst);
            async move { Ok::<_, Infallible>(current == mine) }
        };

        assert!(matches!(
            fenced_put(store.as_ref(), &data, "a1".into(), &fast(), || holds(1)).await,
            Ok(Fenced::Written(_))
        ));
        // Another holder takes over and writes.
        generation.store(2, Ordering::SeqCst);
        assert!(matches!(
            fenced_put(store.as_ref(), &data, "b".into(), &fast(), || holds(2)).await,
            Ok(Fenced::Written(_))
        ));
        assert!(matches!(
            fenced_put(store.as_ref(), &data, "a2".into(), &fast(), || holds(1)).await,
            Ok(Fenced::Refused)
        ));
        let bytes = store
            .get(&data)
            .await
            .expect("stored")
            .bytes()
            .await
            .expect("read");
        assert_eq!(&bytes[..], b"b");
    }

    /// The former holder checks while it still holds the state, then the new
    /// holder takes over and writes before the former holder's write arrives:
    /// that write conflicts, and its retry is refused.
    #[tokio::test]
    async fn a_write_checked_before_a_takeover_does_not_overwrite_the_new_holder() {
        let store = memory();
        let data = Path::from("state/result.arrow");
        let took_over = AtomicBool::new(false);
        let checks = AtomicU64::new(0);

        let result = fenced_put(store.as_ref(), &data, "former".into(), &fast(), || {
            let first = checks.fetch_add(1, Ordering::SeqCst) == 0;
            let store = Arc::clone(&store);
            let data = data.clone();
            let took_over = &took_over;
            async move {
                if first {
                    // The takeover and the new holder's write land between
                    // this check and the former holder's write.
                    took_over.store(true, Ordering::SeqCst);
                    store
                        .put(&data, "new".into())
                        .await
                        .expect("the new holder writes");
                    return Ok::<_, Infallible>(true);
                }
                Ok(!took_over.load(Ordering::SeqCst))
            }
        })
        .await
        .expect("fenced put");
        assert!(matches!(result, Fenced::Refused), "{result:?}");
        assert_eq!(checks.load(Ordering::SeqCst), 2);
        let bytes = store
            .get(&data)
            .await
            .expect("stored")
            .bytes()
            .await
            .expect("read");
        assert_eq!(&bytes[..], b"new");
    }

    /// An in-memory store that yields before every read and write, so holders
    /// polled together all read before any of them writes.
    #[derive(Debug)]
    struct Yielding(InMemory);

    impl std::fmt::Display for Yielding {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Yielding")
        }
    }

    #[async_trait]
    impl ObjectStore for Yielding {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<PutResult> {
            tokio::task::yield_now().await;
            self.0.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            tokio::task::yield_now().await;
            self.0.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.0.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }
}
