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
//!   The floor counts every member head this process has opened, as a writer
//!   or as a replica.
//! - **A dataset's own lease too.** The holder of a shared consumer's unit
//!   also holds each member's own lease (`<log>/leases/writer.json`, the lease
//!   an instance that does not share the consumer writes the dataset under),
//!   and takes a member over only while it holds both. Instances that disagree
//!   on which datasets share a consumer, as during a rollout of a Spicepod
//!   edit, then never both write one dataset: the one that finds the
//!   dataset's own lease held elsewhere stands by for the whole unit.
//! - **An incomplete unit waits.** A unit whose members have not all opened
//!   keeps its data writes closed, but neither takes its members over again on
//!   each renewal nor ignores a handoff request.
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
        let shared = self.groups.consumer_of(dataset).is_some();
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
            member_leases: Mutex::new(BTreeMap::new()),
            shared,
            member_held_elsewhere: AtomicBool::new(false),
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
    // A digest whose output is specified (BLAKE3), so every instance names the
    // same object whatever build it runs; `DefaultHasher`'s output may change
    // between Rust releases.
    let digest = blake3::hash(consumer.as_bytes()).to_hex();
    format!("consumer-{slug}-{}", &digest[..16])
}

/// The log prefix `prefix` was recorded from: a `Path`'s own string, parsed
/// back as it is rather than encoded again, which would name another object
/// for a part holding an encoded character.
fn stored_prefix(prefix: &str) -> Path {
    Path::parse(prefix).unwrap_or_else(|_| Path::from(prefix))
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
    /// Each member's own lease, by log prefix, held beside a shared
    /// consumer's unit lease; empty for a dataset's own unit, whose lease
    /// is that one.
    member_leases: Mutex<BTreeMap<String, Lease>>,
    /// Whether the unit is a shared consumer's, whose members' own leases it
    /// holds too.
    shared: bool,
    /// Whether another instance writes a member under its own lease, as last
    /// found, so the change is logged once.
    member_held_elsewhere: AtomicBool,
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
            .map(|prefix| stored_prefix(prefix.as_str()))
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
        // Every member head counts toward the next generation, a replica's
        // too: it may take the member over later.
        self.prefixes.lock().insert(prefix.to_string());
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
        if !self.take_member_leases().await? {
            return Ok(OpenAs::Replica);
        }
        Ok(OpenAs::Writer {
            generation,
            valid_until: held.valid_until.unwrap_or_else(Instant::now),
        })
    }

    /// The own lease of the member log at `prefix`, held beside a shared
    /// consumer's unit lease.
    fn member_lease(&self, prefix: &str) -> Lease {
        self.member_leases
            .lock()
            .entry(prefix.to_string())
            .or_insert_with(|| Lease::new(self.lease.holder().clone(), self.lease.duration()))
            .clone()
    }

    /// Create, renew or take over the own lease of the member log at
    /// `prefix`. Returns whether this process holds it: `false` when another
    /// instance does, which writes that dataset without sharing this unit's
    /// consumer. Always `true` for a dataset's own unit, whose lease it is.
    async fn take_member_lease(&self, prefix: &str) -> CatalogResult<bool> {
        if !self.shared {
            return Ok(true);
        }
        let path = stored_prefix(prefix).join("leases").join("writer.json");
        let role = self
            .member_lease(prefix)
            .acquire(
                self.store.as_ref(),
                &path,
                &object_store_occ::ConflictRetry::default(),
                |previous| async move {
                    Ok::<_, std::convert::Infallible>(object_store_occ::lease::next_generation(
                        previous,
                    ))
                },
            )
            .await
            .map_err(|error| {
                invalid(format!(
                    "{} could not read or write the writer lease of its member at '{prefix}': {error}",
                    self.subject()
                ))
            })?;
        Ok(matches!(role, Role::Holder { .. }))
    }

    /// Take the own lease of every member log opened, all or none. When
    /// another instance holds any of them, this process takes none and
    /// releases those it holds, so that instance can write every dataset it
    /// writes on its own rather than wait for this one. Returns whether this
    /// process holds all of them.
    async fn take_member_leases(&self) -> CatalogResult<bool> {
        if !self.shared {
            return Ok(true);
        }
        let prefixes: Vec<String> = self.prefixes.lock().iter().cloned().collect();
        // Read every lease before taking any, so an instance writing one of
        // the datasets on its own is never left waiting for another.
        for prefix in &prefixes {
            let path = stored_prefix(prefix).join("leases").join("writer.json");
            let elsewhere = self
                .member_lease(prefix)
                .held_elsewhere(self.store.as_ref(), &path)
                .await
                .map_err(|error| {
                    invalid(format!(
                        "{} could not read the writer lease of its member at '{prefix}': {error}",
                        self.subject()
                    ))
                })?;
            if elsewhere.is_some() {
                self.release_member_leases(None).await;
                return Ok(false);
            }
        }
        for prefix in &prefixes {
            if !self.take_member_lease(prefix).await? {
                // Taken by another instance since the read.
                self.release_member_leases(None).await;
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Release, or hand to `to`, every member's own lease this process wrote.
    async fn release_member_leases(&self, to: Option<&str>) {
        let leases: Vec<(String, Lease)> = self
            .member_leases
            .lock()
            .iter()
            .map(|(prefix, lease)| (prefix.clone(), lease.clone()))
            .collect();
        for (prefix, lease) in leases {
            let path = stored_prefix(&prefix).join("leases").join("writer.json");
            if let Err(error) = lease.release_to(self.store.as_ref(), &path, to).await {
                tracing::debug!("Failed to hand back writer lease {path}: {error}");
            }
        }
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

    /// Whether every member opened so far is written by this process at
    /// `generation`: nothing to take over again, though the unit may still
    /// lack members ([`Self::complete`]).
    async fn written_at(&self, generation: u64) -> bool {
        if generation == 0 {
            return false;
        }
        for member in self.members() {
            if member.writer_generation().await != Some(generation) {
                return false;
            }
        }
        true
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
            #[cfg(test)]
            if let Some(hook) = member.inner.after_writer_gate_open.lock().take() {
                hook(&member.inner);
            }
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
                self.release_member_leases(None).await;
            }
            Found::Holder {
                generation,
                fresh,
                valid_until,
            } => {
                *self.holder.lock() = Some(self.identity().to_string());
                match self.take_member_leases().await {
                    Ok(true) => {
                        if self.member_held_elsewhere.swap(false, Ordering::AcqRel) {
                            tracing::info!(
                                "{} now holds the writer lease of each of its datasets, so this instance takes them over",
                                self.subject()
                            );
                        }
                    }
                    Ok(false) => {
                        // Another instance writes a member under that
                        // dataset's own lease: it does not share this unit's
                        // consumer. This instance writes none of the unit
                        // until that lease is released or lapses.
                        self.close_writes();
                        for member in self.members() {
                            member.step_down(None).await;
                        }
                        held.generation = generation;
                        drop(held);
                        if !self.member_held_elsewhere.swap(true, Ordering::AcqRel) {
                            tracing::warn!(
                                "{} is not written by this instance, so none of its datasets ingests here: another instance writes one of them under that dataset's own writer lease, as an instance whose Spicepod does not name the same `pg_replication_slot` for it does. Run the same Spicepod on every instance on this location",
                                self.subject()
                            );
                        }
                        self.answer_handoff_request().await;
                        return;
                    }
                    Err(error) => {
                        tracing::warn!("{error}");
                        return;
                    }
                }
                if !fresh && self.written_at(generation).await {
                    // A unit that still lacks members keeps its writes closed,
                    // and still answers handoff requests.
                    if self.complete(generation).await {
                        self.open_writes(Some(valid_until));
                    }
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
        self.release_member_leases(to).await;
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

    /// Instances running different builds name one consumer's lease the same
    /// object, so its name is pinned.
    #[test]
    fn a_consumer_lease_name_is_the_same_in_every_build() {
        assert_eq!(
            super::consumer_file("postgres slot orders_slot"),
            "consumer-postgres-slot-orders-slot-9fdd25a64dea6427"
        );
    }

    /// A recorded prefix names the same object it was recorded from, even when
    /// a part of it holds a character the path encodes.
    #[test]
    fn a_recorded_prefix_names_the_log_it_was_recorded_from() {
        let prefix = object_store::path::Path::from("cayenne/v1")
            .join("datasets")
            .join("orders%2025-0193");
        assert_eq!(super::stored_prefix(prefix.as_ref()), prefix);
        assert_ne!(object_store::path::Path::from(prefix.as_ref()), prefix);
    }

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

    mod instances {
        use super::super::super::{Access, Domain, LeaseSettings, NativeCatalogConfig, WriteUnits};
        use super::super::{HandoffRequest, Units, request_path};
        use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
        use std::sync::Arc;

        const LEASE: std::time::Duration = std::time::Duration::from_secs(60);

        fn config(identity: &'static str) -> NativeCatalogConfig {
            NativeCatalogConfig {
                access: Access::Leased(LeaseSettings {
                    duration: LEASE,
                    identity: Some(identity),
                }),
                ..NativeCatalogConfig::default()
            }
        }

        fn units(
            store: &Arc<dyn ObjectStore>,
            identity: &'static str,
            groups: WriteUnits,
        ) -> Arc<Units> {
            Units::new(
                Arc::clone(store),
                Path::from("leases"),
                LeaseSettings {
                    duration: LEASE,
                    identity: Some(identity),
                },
                groups,
            )
        }

        /// Dataset `dataset`'s log at its own prefix, opened (created when
        /// `create`) by the instance `units` belongs to, and joined to its unit.
        async fn open(
            store: &Arc<dyn ObjectStore>,
            units: &Arc<Units>,
            identity: &'static str,
            dataset: &str,
            create: bool,
        ) -> Arc<Domain> {
            let prefix = Path::from(dataset);
            let store = Arc::clone(store);
            let opened = if create {
                Domain::create_with_config(
                    store,
                    prefix,
                    config(identity),
                    true,
                    None,
                    dataset,
                    Some(units),
                )
                .await
            } else {
                Domain::open_with_config(
                    store,
                    prefix,
                    config(identity),
                    true,
                    None,
                    dataset,
                    Some(units),
                )
                .await
            };
            let domain = Arc::new(opened.expect("open the dataset log"));
            domain.join_unit().await;
            domain
        }

        async fn head(store: &Arc<dyn ObjectStore>, dataset: &str) -> Option<u64> {
            object_store_occ::headlog::head_generation(store.as_ref(), &Path::from(dataset))
                .await
                .expect("read the head")
        }

        async fn keep(domain: &Domain) {
            domain
                .unit
                .as_ref()
                .expect("a leased log has a unit")
                .keep()
                .await;
        }

        /// A unit one of whose members never opens keeps its writes closed,
        /// but does not take its open member over again on every renewal.
        #[tokio::test]
        async fn a_unit_missing_a_member_keeps_its_generation_and_its_writes_closed() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let units = units(
                &store,
                "w",
                WriteUnits::default().with_consumer("s", &["a", "b"]),
            );
            let a = open(&store, &units, "w", "a", true).await;
            let opened_at = a.writer_generation().await;
            assert_eq!(opened_at, Some(1));
            for tick in 1..=5 {
                keep(&a).await;
                assert_eq!(
                    (
                        a.data_writes.allows(),
                        a.writer_generation().await,
                        head(&store, "a").await
                    ),
                    (false, Some(1), Some(1)),
                    "renewal {tick}"
                );
            }
        }

        /// A unit one of whose members never opens still hands itself to an
        /// instance that asks for it.
        #[tokio::test]
        async fn a_unit_missing_a_member_answers_a_handoff_request() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let groups = WriteUnits::default().with_consumer("s", &["a", "b"]);
            let holder_units = units(&store, "w", groups.clone());
            let asker_units = units(&store, "r", groups);
            let held = open(&store, &holder_units, "w", "a", true).await;
            let asker = open(&store, &asker_units, "r", "a", false).await;
            assert_eq!(asker.writer_generation().await, None);
            let unit = held.unit.as_ref().expect("a leased log has a unit");
            store
                .put(
                    &request_path(&unit.path),
                    PutPayload::from(
                        serde_json::to_vec(&HandoffRequest::new("r")).expect("serialize"),
                    ),
                )
                .await
                .expect("write the handoff request");
            keep(&held).await;
            keep(&asker).await;
            assert_eq!(
                (
                    held.writer_generation().await,
                    asker.writer_generation().await
                ),
                (None, Some(2)),
                "the unit was not handed to the instance that asked for it"
            );
            let _ = (holder_units, asker_units);
        }

        /// Two instances that disagree on whether a dataset shares a consumer
        /// never both write it, whichever opens it first, and stop retaking
        /// it from each other.
        #[tokio::test]
        async fn instances_that_group_a_dataset_differently_never_both_write_it() {
            for grouped_first in [false, true] {
                let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let own = units(&store, "own", WriteUnits::default());
                let shared = units(
                    &store,
                    "shared",
                    WriteUnits::default().with_consumer("s", &["a"]),
                );
                let (first, second) = if grouped_first {
                    let s = open(&store, &shared, "shared", "a", true).await;
                    (s, open(&store, &own, "own", "a", false).await)
                } else {
                    let o = open(&store, &own, "own", "a", true).await;
                    (o, open(&store, &shared, "shared", "a", false).await)
                };
                let first_generation = first.writer_generation().await;
                assert!(first_generation.is_some(), "grouped first: {grouped_first}");
                for tick in 1..=4 {
                    keep(&first).await;
                    keep(&second).await;
                    assert_eq!(
                        (
                            first.data_writes.allows(),
                            first.writer_generation().await,
                            second.data_writes.allows(),
                            second.writer_generation().await,
                            head(&store, "a").await,
                        ),
                        (true, first_generation, false, None, first_generation),
                        "grouped first: {grouped_first}, renewal {tick}"
                    );
                }
                first
                    .set_sidecar_value("k", Some(b"v".to_vec()))
                    .await
                    .expect("the only writer commits");
                let _ = (own, shared);
            }
        }

        /// An instance that writes one dataset of a shared consumer on its own
        /// (its Spicepod does not share it) is handed the dataset's slot-mates
        /// the other instance had taken, rather than both waiting on each other.
        #[tokio::test]
        async fn an_instance_writing_a_dataset_on_its_own_is_left_its_slot_mates() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let own = units(&store, "own", WriteUnits::default());
            let shared = units(
                &store,
                "shared",
                WriteUnits::default().with_consumer("s", &["a", "b"]),
            );
            let shared_a = open(&store, &shared, "shared", "a", true).await;
            let own_b = open(&store, &own, "own", "b", true).await;
            let own_a = open(&store, &own, "own", "a", false).await;
            let shared_b = open(&store, &shared, "shared", "b", false).await;
            for tick in 1..=3 {
                keep(&shared_a).await;
                keep(&own_a).await;
                keep(&own_b).await;
                assert_eq!(
                    (
                        own_a.data_writes.allows(),
                        own_b.data_writes.allows(),
                        shared_a.data_writes.allows(),
                        shared_b.data_writes.allows(),
                        shared_a.writer_generation().await,
                        shared_b.writer_generation().await,
                    ),
                    (true, true, false, false, None, None),
                    "renewal {tick}"
                );
            }
            let _ = (own, shared);
        }

        /// A member a shared unit's instance opened as a replica (another
        /// instance held the unit) counts toward its next generation, so its
        /// first takeover exceeds a head another lease raised meanwhile.
        #[tokio::test]
        async fn a_takeover_exceeds_the_head_of_a_member_opened_as_a_replica() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let groups = WriteUnits::default().with_consumer("s", &["a"]);
            let own = units(&store, "own", WriteUnits::default());
            let first_shared = units(&store, "z", groups.clone());
            let second_shared = units(&store, "y", groups);
            let alone = open(&store, &own, "own", "a", true).await;
            let holder = open(&store, &first_shared, "z", "a", false).await;
            let standby = open(&store, &second_shared, "y", "a", false).await;
            assert_eq!(
                (
                    holder.writer_generation().await,
                    standby.writer_generation().await
                ),
                (None, None)
            );
            alone.take_over_at(50).await.expect("raise the head");
            alone
                .unit
                .as_ref()
                .expect("a leased log has a unit")
                .hand_over(None)
                .await;
            holder
                .unit
                .as_ref()
                .expect("a leased log has a unit")
                .hand_over(None)
                .await;
            keep(&standby).await;
            assert_eq!(
                (
                    standby.writer_generation().await,
                    standby.data_writes.allows(),
                    head(&store, "a").await
                ),
                (Some(51), true, Some(51))
            );
            let _ = (own, first_shared, second_shared);
        }
    }
}
