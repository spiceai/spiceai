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

//! Writer leases on write units.
//!
//! A write unit is what one writer holds a lease on: a dataset, or a change
//! source's consumer that several datasets share (one PostgreSQL replication
//! slot named by each of them). Every log of a unit's member datasets is taken
//! over under the unit lease's generation, and its data writes open only once
//! every member is taken over, so a shared consumer is never split between
//! instances, nor started for some of its datasets only.
//!
//! - **One generation.** A takeover first gets a fresh lease generation (a
//!   lease created or taken over gets one; otherwise the holder advances its
//!   own record), then takes every member's head over at exactly that number.
//!   The lease's next generation always exceeds every member head's, so the
//!   head refuses a writer the lease has moved past at its next publication.
//! - **Handoff.** An instance that writes fewer units than its share asks the
//!   instance writing the most for one, by writing a request object beside
//!   that unit's lease. The holder reads it at its next renewal, stops the
//!   unit's data writes, commits what it accepted, and hands the lease to the
//!   requester, which takes it over at once. Each instance asks at most once,
//!   and hands over at most one unit, per lease duration.
//! - **Shutdown.** A graceful shutdown hands every unit it writes back the
//!   same way before anything else stops, so another instance takes over
//!   without waiting for the leases to lapse.

use super::{Domain, LeaseSettings, invalid, subject};
use crate::catalog::CatalogResult;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, UpdateVersion, path::Path};
use object_store_occ::lease::{Acquired, Holder, Lease, Role};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

/// Which datasets share a change source's consumer, so one writer holds them
/// together: each dataset name, to its consumer's identity. Derived from the
/// Spicepod, so every instance running it computes the same groups.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteUnits {
    consumers: BTreeMap<String, Arc<str>>,
}

impl WriteUnits {
    /// Datasets `datasets` share the consumer `consumer`.
    #[must_use]
    pub fn with_consumer(mut self, consumer: &str, datasets: &[&str]) -> Self {
        let consumer: Arc<str> = Arc::from(consumer);
        for dataset in datasets {
            self.consumers
                .insert((*dataset).to_string(), Arc::clone(&consumer));
        }
        self
    }

    /// Dataset `dataset` reads from the consumer `consumer`, which other
    /// datasets may share.
    pub fn insert(&mut self, dataset: impl Into<String>, consumer: impl Into<Arc<str>>) {
        self.consumers.insert(dataset.into(), consumer.into());
    }

    fn consumer_of(&self, dataset: &str) -> Option<&Arc<str>> {
        self.consumers.get(dataset)
    }

    fn members_of(&self, consumer: &str) -> BTreeSet<String> {
        self.consumers
            .iter()
            .filter(|(_, of)| of.as_ref() == consumer)
            .map(|(dataset, _)| dataset.clone())
            .collect()
    }
}

/// A handoff request, as stored beside a unit's lease.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct HandoffRequest {
    requester: String,
    /// Unique to each request, so a repeated request is a new version of the
    /// object, even on a store whose version is a hash of the content: the
    /// holder answers each version once.
    #[serde(default)]
    request_id: String,
}

impl HandoffRequest {
    fn new(requester: &str) -> Self {
        Self {
            requester: requester.to_string(),
            request_id: uuid::Uuid::now_v7().to_string(),
        }
    }
}

/// Every write unit of one catalog: their leases, and the balancing that
/// spreads them over the instances on the location.
pub(super) struct Units {
    store: Arc<dyn ObjectStore>,
    /// `leases/` beside the registry: the leases of consumer units.
    leases: Path,
    settings: LeaseSettings,
    identity: Arc<str>,
    groups: WriteUnits,
    by_key: Mutex<HashMap<String, Weak<UnitLease>>>,
    /// Set by a graceful shutdown: no unit is acquired or renewed again.
    closing: AtomicBool,
    /// The handoff request this instance last wrote, while it waits for it.
    requested: Mutex<Option<Path>>,
}

impl Units {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        leases: Path,
        settings: LeaseSettings,
        groups: WriteUnits,
    ) -> Arc<Self> {
        let identity: Arc<str> = Arc::from(
            settings
                .identity
                .unwrap_or_else(util::instance_id::instance_id),
        );
        Arc::new(Self {
            store,
            leases,
            settings,
            identity,
            groups,
            by_key: Mutex::new(HashMap::new()),
            closing: AtomicBool::new(false),
            requested: Mutex::new(None),
        })
    }

    /// The unit dataset `dataset`'s log at `prefix` is written under: its
    /// consumer's, when it shares one, or else its own. The registry (`""`)
    /// is a unit of its own that is never balanced.
    pub(super) fn unit_for(self: &Arc<Self>, dataset: &str, prefix: &Path) -> Arc<UnitLease> {
        let (key, path, expected, label, balanced) = match self.groups.consumer_of(dataset) {
            Some(consumer) => {
                let file = consumer_file(consumer);
                (
                    format!("consumer:{consumer}"),
                    self.leases.clone().join(format!("{file}.json")),
                    self.groups.members_of(consumer),
                    Arc::<str>::from(consumer.as_ref()),
                    true,
                )
            }
            None => (
                format!("log:{prefix}"),
                prefix.clone().join("leases").join("writer.json"),
                BTreeSet::from([dataset.to_string()]),
                Arc::<str>::from(dataset),
                !dataset.is_empty(),
            ),
        };
        let mut by_key = self.by_key.lock();
        if let Some(unit) = by_key.get(&key).and_then(Weak::upgrade) {
            return unit;
        }
        let holder = Holder::new(Arc::clone(&self.identity), uuid::Uuid::now_v7().to_string());
        let request = request_path(&path);
        let unit = Arc::new(UnitLease {
            units: Arc::downgrade(self),
            lease: Lease::new(holder, self.settings.duration),
            store: Arc::clone(&self.store),
            path,
            request,
            label,
            expected,
            balanced,
            prefixes: Mutex::new(BTreeSet::new()),
            members: Mutex::new(Vec::new()),
            held: tokio::sync::Mutex::new(Held::default()),
            holder: Mutex::new(None),
            handled_request: Mutex::new(None),
            last_handoff: Mutex::new(None),
            keeper: AtomicBool::new(false),
            deadline: Mutex::new(None),
        });
        by_key.retain(|_, unit| unit.strong_count() > 0);
        by_key.insert(key, Arc::downgrade(&unit));
        unit
    }

    fn live_units(&self) -> Vec<Arc<UnitLease>> {
        self.by_key
            .lock()
            .values()
            .filter_map(Weak::upgrade)
            .collect()
    }

    pub(super) fn closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// Hand back every unit this instance writes, before anything else of a
    /// graceful shutdown stops: each stops its data writes, commits what it
    /// accepted and releases its lease, so another instance takes over at its
    /// next renewal. No unit is acquired or renewed afterwards.
    pub(super) async fn release_all(&self) {
        self.closing.store(true, Ordering::Release);
        let units = self.live_units();
        futures::future::join_all(units.iter().map(|unit| unit.hand_over(None))).await;
        let requested = self.requested.lock().take();
        if let Some(request) = requested
            && let Err(error) = self.store.delete(&request).await
        {
            tracing::debug!("Failed to withdraw handoff request {request}: {error}");
        }
    }

    /// Ask for one unit when this instance writes fewer than its share, at
    /// most once per lease duration, for as long as the catalog is open.
    pub(super) fn balance_while_open(self: &Arc<Self>) {
        let units = Arc::downgrade(self);
        let every = self.settings.duration;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let Some(units) = units.upgrade() else {
                    break;
                };
                if units.closing() {
                    break;
                }
                units.balance().await;
            }
        });
    }

    /// One balancing step: when this instance writes fewer units than its
    /// share, ask the instance that writes the most for one of them. Its share
    /// is the units divided by the instances that hold any, itself included.
    async fn balance(&self) {
        let units: Vec<Arc<UnitLease>> = self
            .live_units()
            .into_iter()
            .filter(|unit| unit.balanced)
            .collect();
        let held: Vec<(Arc<UnitLease>, String)> = units
            .iter()
            .filter_map(|unit| unit.holder.lock().clone().map(|h| (Arc::clone(unit), h)))
            .collect();
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        counts.insert(self.identity.as_ref(), 0);
        for (_, holder) in &held {
            *counts.entry(holder.as_str()).or_default() += 1;
        }
        let share = held.len() / counts.len();
        let mine = counts.get(self.identity.as_ref()).copied().unwrap_or(0);
        if mine >= share {
            let withdrawn = self.requested.lock().take();
            if let Some(request) = withdrawn
                && let Err(error) = self.store.delete(&request).await
            {
                tracing::debug!("Failed to withdraw handoff request {request}: {error}");
            }
            return;
        }
        // The instance writing the most; the first by name on a tie, so
        // every instance that asks picks the same one.
        let Some((busiest, most)) = counts
            .iter()
            .filter(|(holder, _)| **holder != self.identity.as_ref())
            .max_by(|(a, x), (b, y)| x.cmp(y).then_with(|| b.cmp(a)))
            .map(|(holder, count)| ((*holder).to_string(), *count))
        else {
            return;
        };
        // Moving one unit must narrow the gap, or leases would bounce.
        if most <= mine + 1 {
            return;
        }
        let Some(unit) = held
            .iter()
            .filter(|(_, holder)| *holder == busiest)
            .map(|(unit, _)| unit)
            .min_by(|a, b| a.label.cmp(&b.label))
        else {
            return;
        };
        let body = match serde_json::to_vec(&HandoffRequest::new(&self.identity)) {
            Ok(body) => body,
            Err(error) => {
                tracing::debug!("Failed to serialize a handoff request: {error}");
                return;
            }
        };
        match self.store.put(&unit.request, PutPayload::from(body)).await {
            Ok(_) => {
                tracing::debug!(
                    "{} asked instance '{busiest}' to hand it over: this instance writes {mine} of {} units, below its share of {share}",
                    unit.subject(),
                    held.len(),
                );
                *self.requested.lock() = Some(unit.request.clone());
            }
            Err(error) => tracing::debug!(
                "Failed to ask instance '{busiest}' to hand over {}: {error}",
                unit.subject()
            ),
        }
    }
}

/// A file name for a consumer's lease: readable, and unique to the consumer.
fn consumer_file(consumer: &str) -> String {
    use std::hash::{Hash, Hasher};
    let slug: String = consumer
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .take(40)
        .collect();
    // A stable digest, so every instance names the same object.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    consumer.hash(&mut hasher);
    format!("consumer-{slug}-{:016x}", hasher.finish())
}

/// The handoff request beside the lease at `lease`.
fn request_path(lease: &Path) -> Path {
    let mut parts: Vec<_> = lease.parts().collect();
    let file = parts
        .pop()
        .map(|part| part.as_ref().trim_end_matches(".json").to_string())
        .unwrap_or_default();
    Path::from_iter(parts).join(format!("{file}-request.json"))
}

/// The lease this process holds on a unit.
#[derive(Default)]
struct Held {
    /// The generation every member's head is taken over at; `0` when this
    /// process does not hold the lease.
    generation: u64,
    /// When the lease could lapse unless renewed.
    valid_until: Option<Instant>,
}

/// What a member's log opens as.
pub(super) enum OpenAs {
    /// The writer, taking its head over at this generation, with writes held
    /// until `valid_until`.
    Writer {
        generation: u64,
        valid_until: Instant,
    },
    Replica,
}

/// A writer lease on one write unit, shared by its member datasets' logs.
pub(super) struct UnitLease {
    units: Weak<Units>,
    lease: Lease,
    store: Arc<dyn ObjectStore>,
    path: Path,
    request: Path,
    /// The dataset of a dataset unit, or the consumer of a shared one.
    label: Arc<str>,
    /// Every dataset the unit holds, by name.
    expected: BTreeSet<String>,
    /// Counted when spreading units over instances.
    balanced: bool,
    /// The log prefix of every member opened, for the generation's floor.
    prefixes: Mutex<BTreeSet<String>>,
    members: Mutex<Vec<Weak<Domain>>>,
    held: tokio::sync::Mutex<Held>,
    /// The instance that held the lease when it was last read.
    holder: Mutex<Option<String>>,
    /// The version of the last handoff request this process answered.
    handled_request: Mutex<Option<UpdateVersion>>,
    /// When this process last handed the unit over.
    last_handoff: Mutex<Option<Instant>>,
    keeper: AtomicBool,
    /// When the lease this process holds could lapse unless renewed, read
    /// without waiting for a renewal in flight.
    deadline: Mutex<Option<Instant>>,
}

/// What reading the lease found.
enum Found {
    /// This process holds the lease under `generation`; `fresh` when no member
    /// has been taken over at it yet.
    Holder {
        generation: u64,
        fresh: bool,
        valid_until: Instant,
    },
    Standby {
        holder: Option<String>,
    },
}

impl UnitLease {
    fn subject(&self) -> String {
        if self.expected.len() == 1
            && let Some(dataset) = self.expected.first()
        {
            return subject(dataset);
        }
        format!(
            "The change source consumer '{}' of datasets {} (cayenne)",
            self.label,
            self.expected
                .iter()
                .map(|dataset| format!("'{dataset}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn identity(&self) -> &str {
        self.lease.holder().identity()
    }

    /// How often the keeper runs.
    fn renew_every(&self) -> std::time::Duration {
        self.lease.duration() / 3
    }

    /// The generation after `previous` and after every member head's, so a
    /// takeover at it fences every writer before it, whichever lease it held.
    async fn next_generation(&self, previous: u64, also: Option<&Path>) -> CatalogResult<u64> {
        let mut prefixes: Vec<Path> = self
            .prefixes
            .lock()
            .iter()
            .map(|prefix| Path::from(prefix.as_str()))
            .collect();
        prefixes.extend(also.cloned());
        let mut floor = previous;
        for prefix in prefixes {
            if let Some(generation) =
                object_store_occ::headlog::head_generation(self.store.as_ref(), &prefix)
                    .await
                    .map_err(invalid)?
            {
                floor = floor.max(generation);
            }
        }
        floor
            .checked_add(1)
            .ok_or_else(|| invalid("writer lease generations exhausted"))
    }

    /// Create, renew or take over the lease, and give a holder a generation
    /// no member has been taken over at when it needs one.
    async fn find(&self, held: &mut Held, also: Option<&Path>) -> CatalogResult<Found> {
        let started = Instant::now();
        let role = self
            .lease
            .acquire(
                self.store.as_ref(),
                &self.path,
                &object_store_occ::ConflictRetry::default(),
                |previous| self.next_generation(previous, also),
            )
            .await
            .map_err(|error| {
                invalid(format!(
                    "{} could not read or write its writer lease: {error}",
                    self.subject()
                ))
            })?;
        let valid_until = started + self.lease.duration();
        let (generation, fresh) = match role {
            Role::Holder {
                generation,
                acquired: Acquired::Created | Acquired::TookOver { .. },
            } => (generation, true),
            Role::Holder {
                generation,
                acquired: Acquired::Renewed { by_other_process },
            } if !by_other_process && held.generation == generation => (generation, false),
            // This instance's previous run took the members over at this
            // generation, or this process lost track of it: move on to a
            // generation of its own.
            Role::Holder { .. } => {
                let advanced = self
                    .lease
                    .advance(self.store.as_ref(), &self.path, |previous| {
                        self.next_generation(previous, also)
                    })
                    .await
                    .map_err(|error| {
                        invalid(format!(
                            "{} could not write its writer lease: {error}",
                            self.subject()
                        ))
                    })?;
                let Some(generation) = advanced else {
                    held.generation = 0;
                    *self.deadline.lock() = None;
                    return Ok(Found::Standby { holder: None });
                };
                (generation, true)
            }
            Role::Standby { holder, .. } => {
                held.generation = 0;
                *self.deadline.lock() = None;
                return Ok(Found::Standby { holder });
            }
            Role::Unsupported => {
                return Err(invalid(format!(
                    "{} cannot hold a writer lease, because its location does not support conditional writes",
                    self.subject()
                )));
            }
        };
        held.generation = generation;
        held.valid_until = Some(valid_until);
        *self.deadline.lock() = Some(valid_until);
        Ok(Found::Holder {
            generation,
            fresh,
            valid_until,
        })
    }

    /// Move the lease this process holds to a generation above every member
    /// head's, including the head at `also`.
    async fn advance(&self, held: &mut Held, also: Option<&Path>) -> CatalogResult<Option<u64>> {
        let advanced = self
            .lease
            .advance(self.store.as_ref(), &self.path, |previous| {
                self.next_generation(previous, also)
            })
            .await
            .map_err(|error| {
                invalid(format!(
                    "{} could not write its writer lease: {error}",
                    self.subject()
                ))
            })?;
        held.generation = advanced.unwrap_or(0);
        Ok(advanced)
    }

    /// How the member log at `prefix` opens: as the writer when this process
    /// holds the unit, at a generation above its head's.
    pub(super) async fn open_as(&self, prefix: &Path) -> CatalogResult<OpenAs> {
        if self.units.upgrade().is_none_or(|units| units.closing()) {
            return Ok(OpenAs::Replica);
        }
        let mut held = self.held.lock().await;
        let usable =
            held.generation != 0 && held.valid_until.is_some_and(|until| until > Instant::now());
        let generation = if usable {
            let head_generation =
                object_store_occ::headlog::head_generation(self.store.as_ref(), prefix)
                    .await
                    .map_err(invalid)?;
            if head_generation.is_some_and(|head| head >= held.generation) {
                match self.advance(&mut held, Some(prefix)).await? {
                    Some(generation) => generation,
                    None => return Ok(OpenAs::Replica),
                }
            } else {
                held.generation
            }
        } else {
            match self.find(&mut held, Some(prefix)).await? {
                Found::Holder { generation, .. } => generation,
                Found::Standby { holder } => {
                    *self.holder.lock() = holder;
                    return Ok(OpenAs::Replica);
                }
            }
        };
        *self.holder.lock() = Some(self.identity().to_string());
        self.prefixes.lock().insert(prefix.to_string());
        Ok(OpenAs::Writer {
            generation,
            valid_until: held.valid_until.unwrap_or_else(Instant::now),
        })
    }

    /// Record member `domain`, just opened, and open every member's data
    /// writes once all are written by this process.
    pub(super) async fn join(self: &Arc<Self>, domain: &Arc<Domain>) {
        {
            let mut members = self.members.lock();
            members.retain(|member| member.strong_count() > 0);
            members.push(Arc::downgrade(domain));
        }
        {
            let held = self.held.lock().await;
            if self.complete(held.generation).await {
                self.open_writes(held.valid_until);
            }
        }
        if !self.keeper.swap(true, Ordering::AcqRel) {
            self.keep_while_open();
            self.close_writes_at_deadline();
        }
    }

    /// Close every member's data writes the moment the lease could lapse
    /// unless it was renewed before, however long a renewal in flight takes
    /// (the location may not answer at all). Each member's writes already
    /// refuse past that deadline by themselves; closing them also tells the
    /// runtime, which stops the members' change streams so the instance taking
    /// over finds the source's consumer free.
    fn close_writes_at_deadline(self: &Arc<Self>) {
        let unit = Arc::downgrade(self);
        let every = self.renew_every();
        tokio::spawn(async move {
            loop {
                let deadline = {
                    let Some(unit) = unit.upgrade() else {
                        break;
                    };
                    if unit.members().is_empty() {
                        break;
                    }
                    *unit.deadline.lock()
                };
                let Some(deadline) = deadline else {
                    tokio::time::sleep(every).await;
                    continue;
                };
                if Instant::now() < deadline {
                    tokio::time::sleep_until(deadline.into()).await;
                    continue;
                }
                let Some(unit) = unit.upgrade() else {
                    break;
                };
                // Renewed meanwhile: the deadline moved on.
                {
                    let mut current = unit.deadline.lock();
                    if *current != Some(deadline) {
                        continue;
                    }
                    *current = None;
                }
                // Suspended, not closed: a renewal that lands shows no other
                // instance took the lease meanwhile, so the writes resume
                // under the same generation. One that finds another holder
                // steps every member down.
                for member in unit.members() {
                    member.data_writes.suspend();
                }
                // The failed renewals are already logged, each with its cause.
                tracing::debug!(
                    "{} could not renew its writer lease before it could lapse; it stops writing until it renews",
                    unit.subject()
                );
            }
        });
    }

    fn members(&self) -> Vec<Arc<Domain>> {
        self.members
            .lock()
            .iter()
            .filter_map(Weak::upgrade)
            .collect()
    }

    /// Whether every member is written by this process at `generation`.
    async fn complete(&self, generation: u64) -> bool {
        if generation == 0 {
            return false;
        }
        let members = self.members();
        for dataset in &self.expected {
            let mut written = false;
            for member in members.iter().filter(|m| m.dataset_name() == dataset) {
                written |= member.writer_generation().await == Some(generation);
            }
            if !written {
                return false;
            }
        }
        true
    }

    /// Open every member's data writes, held until `valid_until`. Called only
    /// once all members are written at the generation this process holds:
    /// a consumer is never started for some of its datasets only.
    fn open_writes(&self, valid_until: Option<Instant>) {
        for member in self.members() {
            if let Some(until) = valid_until {
                member.data_writes.hold_until(until);
            }
            member.data_writes.set(true);
        }
    }

    fn close_writes(&self) {
        for member in self.members() {
            member.data_writes.set(false);
        }
    }

    /// Keep the lease while any member is open: renew it, take every member
    /// over once it lapses or is handed to this instance, step down when
    /// another instance took it, and answer handoff requests.
    fn keep_while_open(self: &Arc<Self>) {
        let unit = Arc::downgrade(self);
        let every = self.renew_every();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let Some(unit) = unit.upgrade() else {
                    break;
                };
                if unit.units.upgrade().is_none_or(|units| units.closing()) {
                    break;
                }
                if unit.members().is_empty() {
                    break;
                }
                unit.keep().await;
            }
        });
    }

    /// One renewal: see [`Self::keep_while_open`].
    pub(super) async fn keep(self: &Arc<Self>) {
        let mut held = self.held.lock().await;
        if self.units.upgrade().is_none_or(|units| units.closing()) {
            return;
        }
        let found = match self.find(&mut held, None).await {
            Ok(found) => found,
            Err(error) => {
                // Each member's writes close at the lease's deadline by
                // themselves, so a writer that cannot renew sends no data
                // write once another instance could take over; its commits
                // are fenced by the head.
                tracing::warn!("{error}");
                return;
            }
        };
        match found {
            Found::Standby { holder } => {
                self.holder.lock().clone_from(&holder);
                for member in self.members() {
                    member.step_down(holder.as_deref()).await;
                }
            }
            Found::Holder {
                generation,
                fresh,
                valid_until,
            } => {
                *self.holder.lock() = Some(self.identity().to_string());
                if !fresh && self.complete(generation).await {
                    self.open_writes(Some(valid_until));
                    drop(held);
                    self.answer_handoff_request().await;
                    return;
                }
                // Some member is not written at this generation (a failed
                // takeover, a member that opened as a replica, a commit whose
                // outcome is unknown): every member is taken over again, at a
                // generation none has been at.
                let generation = if fresh {
                    generation
                } else {
                    match self.advance(&mut held, None).await {
                        Ok(Some(generation)) => generation,
                        Ok(None) => return,
                        Err(error) => {
                            tracing::warn!("{error}");
                            return;
                        }
                    }
                };
                self.close_writes();
                for member in self.members() {
                    match member.take_over_at(generation).await {
                        Ok(true) => tracing::info!(
                            "{}",
                            super::took_over_message(
                                member.dataset_name(),
                                &member.location().await
                            )
                        ),
                        Ok(false) => {}
                        Err(error) => tracing::warn!(
                            "{} holds its writer lease but could not take the dataset over, so it keeps serving queries as a replica and retries. Cause: {error}",
                            subject(member.dataset_name())
                        ),
                    }
                }
                held.valid_until = Some(valid_until);
                if self.complete(generation).await {
                    self.open_writes(Some(valid_until));
                }
            }
        }
    }

    /// Hand the unit to the instance whose request is waiting beside the
    /// lease, at most once per lease duration.
    async fn answer_handoff_request(&self) {
        let request = match self.store.get(&self.request).await {
            Ok(request) => request,
            Err(object_store::Error::NotFound { .. }) => return,
            Err(error) => {
                tracing::debug!("Failed to read handoff request {}: {error}", self.request);
                return;
            }
        };
        let version = UpdateVersion {
            e_tag: request.meta.e_tag.clone(),
            version: request.meta.version.clone(),
        };
        if self.handled_request.lock().as_ref() == Some(&version) {
            return;
        }
        if self
            .last_handoff
            .lock()
            .is_some_and(|at| at.elapsed() < self.lease.duration())
        {
            return;
        }
        let Ok(bytes) = request.bytes().await else {
            return;
        };
        *self.handled_request.lock() = Some(version);
        let Ok(HandoffRequest { requester, .. }) = serde_json::from_slice(&bytes) else {
            return;
        };
        if requester == self.identity() {
            return;
        }
        self.hand_over(Some(&requester)).await;
    }

    /// Stop every member's data writes, commit what each accepted, follow the
    /// committed state as a replica, and release the lease: to `to`, which
    /// takes it over at once, or to whichever instance renews first.
    pub(super) async fn hand_over(&self, to: Option<&str>) {
        let mut held = self.held.lock().await;
        if held.generation == 0 {
            return;
        }
        self.close_writes();
        for member in self.members() {
            member.hand_over(to).await;
        }
        held.generation = 0;
        held.valid_until = None;
        *self.deadline.lock() = None;
        *self.last_handoff.lock() = Some(Instant::now());
        match self
            .lease
            .release_to(self.store.as_ref(), &self.path, to)
            .await
        {
            Ok(_) => *self.holder.lock() = to.map(str::to_string),
            Err(error) => {
                tracing::debug!("Failed to hand back writer lease {}: {error}", self.path);
            }
        }
        if to.is_some()
            && let Err(error) = self.store.delete(&self.request).await
        {
            tracing::debug!("Failed to remove handoff request {}: {error}", self.request);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HandoffRequest;

    /// A store whose version is a hash of the content would give a repeated
    /// request the version of the one the holder already answered, and the
    /// holder would never answer it.
    #[test]
    fn each_handoff_request_is_new_content() {
        let first = serde_json::to_vec(&HandoffRequest::new("b")).expect("serialize");
        let second = serde_json::to_vec(&HandoffRequest::new("b")).expect("serialize");
        assert_ne!(first, second);
        let read: HandoffRequest = serde_json::from_slice(&first).expect("parse");
        assert_eq!(read.requester, "b");
    }
}
