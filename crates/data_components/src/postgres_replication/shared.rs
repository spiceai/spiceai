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

//! Share one Postgres replication slot across multiple `refresh_mode: changes`
//! datasets.
//!
//! Each logical replication slot runs its own walsender + decoder over the
//! entire WAL stream on the source server (publication filters apply
//! *post*-decode), and Postgres defaults to `max_replication_slots = 10`.
//! Deployments that CDC-mirror several small tables from one database — e.g.
//! a Spice Cloud per-customer cluster mirroring ~6 control tables — would
//! otherwise burn one slot + one decoder per table.
//!
//! Sharing is implicit: when a dataset names its slot explicitly
//! (`pg_replication_slot`), all datasets on the same
//! `(host, port, database, user, slot)` join a single *shared source* — one
//! replication connection, one slot, one publication covering every member
//! table, with decoded changes routed by `(schema, table)` to each member's
//! accelerator sink. A slot named by only one dataset degenerates to the
//! per-dataset behavior (single member). Datasets without an explicit slot
//! keep their dedicated per-dataset stream and generated slot name.
//!
//! # Consistency model
//!
//! Identical to the per-dataset path: at-least-once across the snapshot/WAL
//! boundary, made convergent by PK-based upsert (`on_conflict: upsert`).
//! A member takes an initial snapshot when the slot was created fresh in this
//! process, or when its table was newly added to the shared publication
//! (late-added dataset). pgoutput decodes with a historic catalog, so a
//! table's WAL on the slot begins at the `ALTER PUBLICATION ... ADD TABLE`
//! commit; the snapshot (taken after the ALTER) covers everything before it
//! and the overlap replays idempotently.
//!
//! # Ack / WAL-retention model
//!
//! `confirmed_flush_lsn` is per-slot, so the shared slot acknowledges the
//! *minimum* durably-applied LSN across all members ([`AckTable`]). Members
//! with no traffic are credited forward on keepalives/commits whenever they
//! have no in-flight envelopes. A stalled or failed member therefore pins WAL
//! retention for the whole slot **by design** — acking past it would lose its
//! changes permanently. The pinning is observable via the existing
//! `dataset_postgres_replication_lag_bytes` metric and a WARN log on detach;
//! restarting spiced (or the member rejoining) heals it by replaying from the
//! held LSN, which every member applies idempotently.
//!
//! # Lifecycle
//!
//! - A member whose receiver is dropped (dataset removed, sink task died) is
//!   detached: routing stops, but its ack floor is held (see above).
//! - A member rejoining in-process reuses its held floor and triggers a pump
//!   reconnect; Postgres resumes from `confirmed_flush_lsn`, replaying the gap
//!   to everyone (idempotent).
//! - Fatal errors (auth, decode, slot dropped) are broadcast to all members.
//! - The pump exits and deregisters once every member has detached; a later
//!   subscriber starts a fresh pump that resumes from the slot.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationEvent};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{
    Error, ReplicationMetricsCollector, ReplicationStreamInput, Result, bootstrap,
    changes::{ChangeOp, DecodedChange},
    client,
    config::ReplicationParams,
    pgoutput, resilience, slot,
};
use crate::cdc::{ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError};
use crate::postgres_replication::pgoutput::RelationId;

/// Bounded per-member delivery queue. When one member's sink stops draining,
/// the pump blocks on its channel and the whole shared stream pauses (in
/// addition to the WAL pinning the ack floor already causes) — bounded memory
/// is preferred over unbounded buffering behind a stalled sink.
const MEMBER_CHANNEL_CAPACITY: usize = 64;

/// Upper bound on how long the pump blocks in `recv()` before re-checking
/// membership (joins, dropped receivers). Idle Postgres servers can go tens
/// of seconds between messages; this keeps membership changes responsive.
const RECV_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Identity of a shared replication source. Datasets whose connection params
/// and slot name produce the same key share one pump.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct SourceKey {
    host: String,
    port: u16,
    database: String,
    user: String,
    slot_name: String,
}

impl SourceKey {
    fn from_params(params: &ReplicationParams) -> Self {
        Self {
            host: params.host.clone(),
            port: params.port,
            database: params.database.clone(),
            user: params.user.clone(),
            slot_name: params.slot_name.clone(),
        }
    }
}

/// `(schema, table)` of a member's source table — the routing key for decoded
/// changes.
type MemberKey = (String, String);

static REGISTRY: LazyLock<Mutex<HashMap<SourceKey, Arc<SharedSource>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Lock helper that shrugs off poisoning: every critical section here is a
/// short read-modify-write over plain data, so a panicking peer cannot leave
/// the map in a logically broken state.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

struct MemberHandle {
    dataset_name: String,
    schema: SchemaRef,
    primary_keys: Vec<String>,
    sender: mpsc::Sender<std::result::Result<ChangeEnvelope, StreamError>>,
    metrics: Arc<ReplicationMetricsCollector>,
}

#[derive(Clone, Copy, Debug)]
struct AckEntry {
    /// Highest commit LSN durably applied by this member's sink.
    committed: u64,
    /// Highest commit LSN delivered into this member's channel.
    delivered: u64,
    /// `false` once the member detached; its `committed` then becomes a held
    /// floor the shared ack can never pass.
    live: bool,
}

/// Per-member LSN accounting. The slot-level acknowledgment
/// (`shared_flush`) is the minimum `committed` over **all** entries — live or
/// held — advanced monotonically.
#[derive(Default)]
struct AckTable {
    entries: Mutex<HashMap<MemberKey, AckEntry>>,
    shared_flush: AtomicU64,
}

impl AckTable {
    /// Register a member (or revive a detached one). A rejoining member keeps
    /// its held `committed` floor — everything after it is about to be
    /// replayed.
    fn register(&self, key: &MemberKey) {
        let at = self.flush_lsn();
        let mut entries = lock(&self.entries);
        entries
            .entry(key.clone())
            .and_modify(|e| {
                e.live = true;
                e.delivered = e.committed;
            })
            .or_insert(AckEntry {
                committed: at,
                delivered: at,
                live: true,
            });
    }

    fn deliver(&self, key: &MemberKey, lsn: u64) {
        let mut entries = lock(&self.entries);
        if let Some(e) = entries.get_mut(key) {
            e.delivered = e.delivered.max(lsn);
        }
    }

    fn commit(&self, key: &MemberKey, lsn: u64) {
        {
            let mut entries = lock(&self.entries);
            if let Some(e) = entries.get_mut(key) {
                e.committed = e.committed.max(lsn);
            }
        }
        self.recompute();
    }

    /// Credit live members with no in-flight envelopes up to `upto` — they
    /// have applied (or never needed) everything before it. Held entries are
    /// never credited: that's the point of the hold.
    fn credit_idle(&self, upto: u64) {
        {
            let mut entries = lock(&self.entries);
            for e in entries.values_mut() {
                if e.live && e.delivered == e.committed {
                    let lsn = e.committed.max(upto);
                    e.committed = lsn;
                    e.delivered = lsn;
                }
            }
        }
        self.recompute();
    }

    fn detach(&self, key: &MemberKey) {
        let mut entries = lock(&self.entries);
        if let Some(e) = entries.get_mut(key) {
            e.live = false;
        }
    }

    /// Recompute the slot-level floor and advance `shared_flush` to it
    /// (monotonic — never regresses).
    fn recompute(&self) {
        let floor = {
            let entries = lock(&self.entries);
            entries.values().map(|e| e.committed).min()
        };
        if let Some(floor) = floor {
            advance_monotonic(&self.shared_flush, floor);
        }
    }

    /// Seed the shared flush with the slot's consistent LSN so we never ack 0.
    fn seed(&self, lsn: u64) {
        advance_monotonic(&self.shared_flush, lsn);
    }

    fn flush_lsn(&self) -> u64 {
        self.shared_flush.load(Ordering::Acquire)
    }
}

fn advance_monotonic(flush: &AtomicU64, to: u64) {
    let mut current = flush.load(Ordering::Relaxed);
    loop {
        if to <= current {
            return;
        }
        match flush.compare_exchange(current, to, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

/// `CommitChange` impl for shared-slot envelopes: advances this member's
/// committed LSN and recomputes the slot-level floor.
struct SharedLsnCommitter {
    ack: Arc<AckTable>,
    key: MemberKey,
    flush_to: u64,
}

#[async_trait]
impl CommitChange for SharedLsnCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.ack.commit(&self.key, self.flush_to);
        Ok(())
    }

    /// Same argument as the per-dataset `LsnCommitter`: the slot retains WAL
    /// until the shared floor advances past this commit, so a crash before a
    /// deferred commit re-streams the un-acked tail. Safe to defer.
    fn supports_deferral(&self) -> bool {
        true
    }
}

/// One shared replication source: registry entry + pump state.
struct SharedSource {
    key: SourceKey,
    /// Connection-level params from the first subscriber. Member-level params
    /// (snapshot batch size, `initial_snapshot`) stay per-member.
    params: ReplicationParams,
    /// Serializes member setup (slot/publication DDL) and pump spawn.
    setup_lock: tokio::sync::Mutex<()>,
    members: Mutex<HashMap<MemberKey, Arc<MemberHandle>>>,
    ack: Arc<AckTable>,
    pump_started: AtomicBool,
    /// Asks the pump to drop its connection and reconnect from the current
    /// shared flush LSN — set when a member joins an already-running pump so
    /// Postgres re-sends `Relation` messages (and replays held WAL on rejoin).
    restart_requested: AtomicBool,
    /// Set when the pump has exited (fatal error or all members detached).
    /// Subscribers seeing this create a fresh source.
    dead: AtomicBool,
    /// Whether the slot was created fresh by this process. Members that join
    /// later must snapshot even if their table already sat in the publication:
    /// a fresh slot has no history for anyone.
    slot_created_fresh: AtomicBool,
    /// Tables whose member detached during this pump's lifetime; a rejoin is
    /// resumed via held-floor replay instead of a snapshot.
    detached: Mutex<HashSet<MemberKey>>,
}

impl SharedSource {
    fn new(key: SourceKey, params: ReplicationParams) -> Self {
        Self {
            key,
            params,
            setup_lock: tokio::sync::Mutex::new(()),
            members: Mutex::new(HashMap::new()),
            ack: Arc::new(AckTable::default()),
            pump_started: AtomicBool::new(false),
            restart_requested: AtomicBool::new(false),
            dead: AtomicBool::new(false),
            slot_created_fresh: AtomicBool::new(false),
            detached: Mutex::new(HashSet::new()),
        }
    }

    fn member(&self, key: &MemberKey) -> Option<Arc<MemberHandle>> {
        lock(&self.members).get(key).cloned()
    }

    fn live_members(&self) -> Vec<(MemberKey, Arc<MemberHandle>)> {
        lock(&self.members)
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect()
    }

    fn live_member_count(&self) -> usize {
        lock(&self.members).len()
    }

    fn for_each_member_metrics(&self, f: impl Fn(&ReplicationMetricsCollector)) {
        for (_, m) in self.live_members() {
            f(&m.metrics);
        }
    }

    /// Detach a member: stop routing to it but hold its ack floor so the slot
    /// never acknowledges past what it durably applied. See the module docs
    /// for why the hold (and the WAL retention it causes) is intentional.
    fn detach_member(&self, key: &MemberKey, reason: &str) {
        let removed = lock(&self.members).remove(key);
        self.ack.detach(key);
        lock(&self.detached).insert(key.clone());
        if let Some(member) = removed {
            tracing::warn!(
                dataset = %member.dataset_name,
                table = %format_member(key),
                slot = %self.key.slot_name,
                reason,
                "shared replication member detached; its last applied LSN now pins WAL \
                 retention for the shared slot until the dataset rejoins or spiced restarts \
                 (watch dataset_postgres_replication_lag_bytes)"
            );
        }
    }

    /// Lazily detach members whose receiving stream has been dropped.
    fn reap_closed_members(&self) {
        let closed: Vec<MemberKey> = lock(&self.members)
            .iter()
            .filter(|(_, m)| m.sender.is_closed())
            .map(|(k, _)| k.clone())
            .collect();
        for key in closed {
            self.detach_member(&key, "changes stream receiver dropped");
        }
    }
}

fn format_member(key: &MemberKey) -> String {
    format!("{}.{}", key.0, key.1)
}

/// Entry point: subscribe one dataset to its shared replication source.
///
/// Mirrors [`super::start_replication_stream`]'s lazy contract — setup runs on
/// first poll and setup errors surface through the stream.
#[must_use]
pub fn subscribe(input: ReplicationStreamInput) -> ChangesStream {
    Box::pin(
        stream::once(async move { subscribe_inner(input).await }).flat_map(|result| match result {
            Ok(stream) => stream,
            Err(e) => stream::once(async move { Err(super::stream_error(&e)) }).boxed(),
        }),
    )
}

async fn subscribe_inner(input: ReplicationStreamInput) -> Result<ChangesStream> {
    let key = SourceKey::from_params(&input.params);

    // A source can die (pump exit) between us fetching it and acquiring its
    // setup lock; retry against a fresh registry entry.
    for _attempt in 0..3 {
        let source = get_or_create_source(&key, &input.params);
        let guard = source.setup_lock.lock().await;
        if source.dead.load(Ordering::Acquire) {
            drop(guard);
            continue;
        }
        return attach_member(&source, input).await;
    }
    Err(Error::SharedSourceUnavailable {
        slot: key.slot_name,
    })
}

fn get_or_create_source(key: &SourceKey, params: &ReplicationParams) -> Arc<SharedSource> {
    let mut registry = lock(&REGISTRY);
    if let Some(existing) = registry.get(key) {
        if !existing.dead.load(Ordering::Acquire) {
            return Arc::clone(existing);
        }
    }
    let source = Arc::new(SharedSource::new(key.clone(), params.clone()));
    registry.insert(key.clone(), Arc::clone(&source));
    source
}

/// Register one member on the source (caller holds the setup lock):
/// validate + publication ADD TABLE + slot create-if-first, decide whether a
/// snapshot is needed, wire the routing channel, and start the pump if this is
/// the first member.
async fn attach_member(
    source: &Arc<SharedSource>,
    input: ReplicationStreamInput,
) -> Result<ChangesStream> {
    let ReplicationStreamInput {
        dataset_name,
        params,
        schema,
        primary_keys,
        schema_name,
        table_name,
        metrics,
    } = input;
    let member_key: MemberKey = (schema_name.clone(), table_name.clone());

    // All members of a shared slot must agree on the publication — the pump
    // opens one replication connection with one publication name.
    if params.publication_name != source.params.publication_name {
        return Err(Error::SharedPublicationMismatch {
            dataset: dataset_name,
            expected: source.params.publication_name.clone(),
            got: params.publication_name,
        });
    }

    if source.member(&member_key).is_some() {
        return Err(Error::SharedTableAlreadySubscribed {
            schema: schema_name,
            table: table_name,
            slot: source.key.slot_name.clone(),
        });
    }

    // Slot + publication DDL (idempotent, retried on transient errors).
    let setup = slot::setup_shared_member(&source.params, &schema_name, &table_name).await?;
    if setup.slot.created_fresh {
        source.slot_created_fresh.store(true, Ordering::Release);
    }
    if setup.slot.consistent_lsn > 0 {
        source.ack.seed(setup.slot.consistent_lsn);
        metrics.set_confirmed_flush_lsn(setup.slot.consistent_lsn);
    }

    let rejoining = lock(&source.detached).remove(&member_key);
    // Snapshot when this slot epoch has no usable history for the table:
    // fresh slot (regardless of leftover publication membership), or table
    // newly added to the publication. A rejoin skips it — the held ack floor
    // guarantees the gap is still in WAL and will be replayed.
    let need_snapshot =
        !rejoining && (source.slot_created_fresh.load(Ordering::Acquire) || setup.table_added);

    let (sender, receiver) = mpsc::channel(MEMBER_CHANNEL_CAPACITY);
    source.ack.register(&member_key);
    lock(&source.members).insert(
        member_key.clone(),
        Arc::new(MemberHandle {
            dataset_name: dataset_name.clone(),
            schema: Arc::clone(&schema),
            primary_keys: primary_keys.clone(),
            sender,
            metrics: Arc::clone(&metrics),
        }),
    );

    tracing::info!(
        dataset = %dataset_name,
        table = %format_member(&member_key),
        slot = %source.key.slot_name,
        publication = %source.params.publication_name,
        snapshot = need_snapshot,
        rejoining,
        members = source.live_member_count(),
        "dataset joined shared replication slot"
    );

    if source.pump_started.swap(true, Ordering::AcqRel) {
        // Pump already running: it must reconnect so Postgres re-sends
        // Relation messages for (and, on rejoin, replays held WAL to) the new
        // member. Joins coalesce — the flag is cleared once per reconnect.
        source.restart_requested.store(true, Ordering::Release);
    } else {
        let pump_source = Arc::clone(source);
        tokio::spawn(run_pump(pump_source));
    }

    // Head of the member's stream: initial snapshot (its last envelope flips
    // is_dataset_ready), or an immediate ready signal when resuming.
    let head: ChangesStream = if need_snapshot && params.initial_snapshot {
        Box::pin(bootstrap::snapshot_stream(bootstrap::SnapshotInput {
            params: params.clone(),
            schema_name,
            table_name,
            dataset_schema: Arc::clone(&schema),
            primary_keys,
            dataset_name,
            metrics,
        })?)
    } else {
        metrics.mark_bootstrap_complete();
        let envelope = crate::cdc::build_ready_signal_envelope(&schema).map_err(|e| {
            Error::SchemaMismatch {
                message: e.to_string(),
            }
        })?;
        Box::pin(stream::once(async move { Ok(envelope) }))
    };

    Ok(Box::pin(head.chain(ReceiverStream::new(receiver))))
}

/// Send a fatal error to every member and terminate the source.
async fn fatal_broadcast(source: &Arc<SharedSource>, message: String) {
    tracing::error!(
        slot = %source.key.slot_name,
        "shared replication stream failed: {message}"
    );
    for (_, member) in source.live_members() {
        let _ = member
            .sender
            .send(Err(StreamError::External(message.clone())))
            .await;
    }
}

/// Send a member-scoped fatal error and detach the member (holding its ack
/// floor — see module docs).
async fn member_fatal(source: &Arc<SharedSource>, key: &MemberKey, message: String) {
    if let Some(member) = source.member(key) {
        let _ = member
            .sender
            .send(Err(StreamError::External(message)))
            .await;
    }
    source.detach_member(key, "fatal member error");
}

/// Mark the source dead and drop it from the registry (only if the registry
/// still points at this instance — a replacement may already exist).
fn finish_pump(source: &Arc<SharedSource>) {
    source.dead.store(true, Ordering::Release);
    let mut registry = lock(&REGISTRY);
    if let Some(current) = registry.get(&source.key) {
        if Arc::ptr_eq(current, source) {
            registry.remove(&source.key);
        }
    }
}

/// Atomically decide whether the pump should exit because every member has
/// detached. Takes the same `setup_lock` that `attach_member` holds while
/// registering, so a subscriber can never register on a source that is
/// concurrently finalizing its own death: either it registers first (we see
/// the member and keep running) or we die first (it observes `dead` under the
/// lock and retries against a fresh source).
async fn try_finish_if_empty(source: &Arc<SharedSource>) -> bool {
    let _guard = source.setup_lock.lock().await;
    source.reap_closed_members();
    if source.live_member_count() == 0 {
        finish_pump(source);
        true
    } else {
        false
    }
}

/// The shared pump: one replication connection driving every member.
///
/// Structure mirrors the per-dataset `client::wal_stream` (reconnect loop with
/// backoff around a recv loop), with three differences: transactions buffer
/// per relation and route per member at Commit, acknowledgment is the
/// [`AckTable`] floor instead of a single atomic, and member joins trigger a
/// reconnect instead of a new connection.
async fn run_pump(source: Arc<SharedSource>) {
    let params = source.params.clone();
    let slot_name = source.key.slot_name.clone();
    let publication_name = params.publication_name.clone();
    let mut backoff = resilience::Backoff::default_for_stream();
    let mut reconnect_attempts: u32 = 0;

    'reconnect: loop {
        source.reap_closed_members();
        if source.live_member_count() == 0 && try_finish_if_empty(&source).await {
            tracing::info!(
                slot = %slot_name,
                "all members detached; shutting down shared replication stream"
            );
            return;
        }
        // Joins that happened before this (re)connect are picked up by it.
        source.restart_requested.store(false, Ordering::Release);

        let config = client::build_replication_config(
            &params,
            &slot_name,
            &publication_name,
            source.ack.flush_lsn(),
        );
        let mut client = match ReplicationClient::connect(config).await {
            Ok(c) => {
                backoff.reset();
                if reconnect_attempts > 0 {
                    tracing::info!(
                        slot = %slot_name,
                        attempts = reconnect_attempts,
                        "shared replication connection resumed"
                    );
                    reconnect_attempts = 0;
                }
                c
            }
            Err(e) if resilience::is_transient_pgwire(&e) => {
                source.for_each_member_metrics(ReplicationMetricsCollector::inc_reconnect);
                reconnect_attempts = reconnect_attempts.saturating_add(1);
                client::log_transient_reconnect(
                    reconnect_attempts,
                    &slot_name,
                    &e.to_string(),
                    backoff.current().as_millis(),
                );
                backoff.wait().await;
                continue 'reconnect;
            }
            Err(e) => {
                fatal_broadcast(&source, format!("fatal replication connect failed: {e}")).await;
                break 'reconnect;
            }
        };

        // Per-connection state: Postgres re-sends Relation messages on a new
        // connection, rebuilding the routes.
        let mut decoder = pgoutput::Decoder::new();
        let mut routes: HashMap<RelationId, MemberKey> = HashMap::new();
        let mut txn: HashMap<RelationId, Vec<DecodedChange>> = HashMap::new();
        let mut txn_open = false;

        'recv: loop {
            if source.restart_requested.swap(false, Ordering::AcqRel) {
                tracing::debug!(
                    slot = %slot_name,
                    "reconnecting shared replication stream to pick up membership change"
                );
                break 'recv;
            }
            source.reap_closed_members();
            if source.live_member_count() == 0 && try_finish_if_empty(&source).await {
                tracing::info!(
                    slot = %slot_name,
                    "all members detached; shutting down shared replication stream"
                );
                return;
            }

            // Bound the wait so membership changes (joins via the restart
            // flag, receiver drops via the reap above) are noticed within
            // ~`RECV_POLL_INTERVAL` even on a quiet server — `recv()` only
            // returns on real server traffic, which can be tens of seconds
            // apart when the source is idle. `recv()` reads an internal
            // channel and is cancel-safe.
            let polled = tokio::time::timeout(RECV_POLL_INTERVAL, client.recv()).await;
            let event = match polled {
                Err(_elapsed) => continue 'recv,
                Ok(Ok(Some(e))) => e,
                // Server closed cleanly (e.g. orderly Postgres shutdown):
                // treat like a transient drop and reconnect — the shared
                // stream is meant to run for the process lifetime.
                Ok(Ok(None)) => break 'recv,
                Ok(Err(e)) => {
                    source.for_each_member_metrics(ReplicationMetricsCollector::inc_recv_error);
                    if resilience::is_transient_pgwire(&e) {
                        source.for_each_member_metrics(ReplicationMetricsCollector::inc_reconnect);
                        reconnect_attempts = reconnect_attempts.saturating_add(1);
                        client::log_transient_reconnect(
                            reconnect_attempts,
                            &slot_name,
                            &e.to_string(),
                            backoff.current().as_millis(),
                        );
                        break 'recv;
                    }
                    fatal_broadcast(&source, format!("replication recv failed: {e}")).await;
                    break 'reconnect;
                }
            };

            match event {
                ReplicationEvent::Begin { .. } => {
                    txn_open = true;
                    txn.clear();
                }
                ReplicationEvent::XLogData { data, wal_end, .. } => {
                    source.for_each_member_metrics(|m| m.set_server_wal_end(wal_end.0));
                    let msg = match decoder.decode(&data) {
                        Ok(m) => m,
                        Err(e) => {
                            source.for_each_member_metrics(
                                ReplicationMetricsCollector::inc_decode_error,
                            );
                            fatal_broadcast(&source, format!("pgoutput decode failed: {e}")).await;
                            break 'reconnect;
                        }
                    };
                    handle_decoded(&source, &mut decoder, &mut routes, &mut txn, msg).await;
                }
                ReplicationEvent::Commit {
                    end_lsn,
                    commit_time_micros,
                    ..
                } => {
                    txn_open = false;
                    deliver_commit(
                        &source,
                        &decoder,
                        &routes,
                        std::mem::take(&mut txn),
                        end_lsn.0,
                        commit_time_micros,
                    )
                    .await;
                    let flush = source.ack.flush_lsn();
                    source.for_each_member_metrics(|m| m.set_confirmed_flush_lsn(flush));
                    client.update_applied_lsn(Lsn(flush));
                }
                ReplicationEvent::KeepAlive { wal_end, .. } => {
                    source.for_each_member_metrics(|m| m.set_server_wal_end(wal_end.0));
                    if !txn_open {
                        source.ack.credit_idle(wal_end.0);
                    }
                    let flush = source.ack.flush_lsn();
                    source.for_each_member_metrics(|m| m.set_confirmed_flush_lsn(flush));
                    client.update_applied_lsn(Lsn(flush));
                }
                ReplicationEvent::Message { .. } => {}
                ReplicationEvent::StoppedAt { reached } => {
                    tracing::info!(
                        slot = %slot_name,
                        reached = ?reached,
                        "shared replication stream stopped at upper bound"
                    );
                    break 'reconnect;
                }
            }
        } // end 'recv

        // Inner loop broke for reconnect (transient error or membership
        // change). On membership change the backoff was just reset by the
        // successful connect, so the wait is the minimal initial delay.
        backoff.wait().await;
    } // end 'reconnect

    // Fatal exit. Take the setup lock so no subscriber is mid-registration,
    // then error any member that registered after the fatal broadcast — it
    // must not be left with a silently idle stream on a dead source.
    let _guard = source.setup_lock.lock().await;
    for (_, member) in source.live_members() {
        let _ = member
            .sender
            .send(Err(StreamError::External(
                "shared replication stream terminated".to_string(),
            )))
            .await;
    }
    finish_pump(&source);
}

/// Apply one decoded pgoutput message to the per-connection routing/buffer
/// state.
async fn handle_decoded(
    source: &Arc<SharedSource>,
    decoder: &mut pgoutput::Decoder,
    routes: &mut HashMap<RelationId, MemberKey>,
    txn: &mut HashMap<RelationId, Vec<DecodedChange>>,
    msg: pgoutput::DecodedMessage,
) {
    use pgoutput::DecodedMessage;
    match msg {
        DecodedMessage::Relation(rel) => {
            let member_key: MemberKey = (rel.namespace.clone(), rel.name.clone());
            let Some(member) = source.member(&member_key) else {
                // No member for this table (e.g. publication membership left
                // over from a removed dataset). Decoded changes are dropped.
                routes.remove(&rel.relation_id);
                tracing::debug!(
                    table = %format_member(&member_key),
                    slot = %source.key.slot_name,
                    "relation in shared publication has no subscribed dataset; ignoring its changes"
                );
                return;
            };
            if let Err(e) =
                client::validate_relation_against_schema(&member.schema, &rel, &member.primary_keys)
            {
                member.metrics.inc_schema_mismatch_error();
                routes.remove(&rel.relation_id);
                member_fatal(
                    source,
                    &member_key,
                    format!("schema mismatch for {}: {e}", member.dataset_name),
                )
                .await;
                return;
            }
            decoder.apply_declared_primary_keys(rel.relation_id, &member.primary_keys);
            routes.insert(rel.relation_id, member_key);
        }
        DecodedMessage::Insert { relation_id, tuple } => {
            if let Some(member_key) = routes.get(&relation_id) {
                if let Some(member) = source.member(member_key) {
                    member.metrics.inc_insert();
                    txn.entry(relation_id).or_default().push(DecodedChange {
                        op: ChangeOp::Create,
                        row: tuple,
                    });
                }
            }
        }
        DecodedMessage::Update {
            relation_id, new, ..
        } => {
            if let Some(member_key) = routes.get(&relation_id) {
                if let Some(member) = source.member(member_key) {
                    member.metrics.inc_update();
                    txn.entry(relation_id).or_default().push(DecodedChange {
                        op: ChangeOp::Update,
                        row: new,
                    });
                }
            }
        }
        DecodedMessage::Delete { relation_id, old } => {
            if let Some(member_key) = routes.get(&relation_id) {
                if let Some(member) = source.member(member_key) {
                    member.metrics.inc_delete();
                    txn.entry(relation_id).or_default().push(DecodedChange {
                        op: ChangeOp::Delete,
                        row: old,
                    });
                }
            }
        }
        DecodedMessage::Truncate { relation_ids } => {
            // Unlike the per-dataset path, multi-relation TRUNCATEs are fine
            // here: each relation routes to its own member.
            for relation_id in relation_ids {
                if let Some(member_key) = routes.get(&relation_id) {
                    if let Some(member) = source.member(member_key) {
                        member.metrics.inc_truncate();
                        txn.entry(relation_id).or_default().push(DecodedChange {
                            op: ChangeOp::Truncate,
                            row: pgoutput::TupleData { columns: vec![] },
                        });
                        tracing::info!(
                            dataset = %member.dataset_name,
                            relation_id,
                            "TRUNCATE from shared postgres replication queued for accelerator"
                        );
                    }
                }
            }
        }
        DecodedMessage::Begin { .. } | DecodedMessage::Commit { .. } | DecodedMessage::Other => {}
    }
}

/// Route a committed transaction's buffered changes to their members, then
/// credit idle members and recompute the shared ack floor.
async fn deliver_commit(
    source: &Arc<SharedSource>,
    decoder: &pgoutput::Decoder,
    routes: &HashMap<RelationId, MemberKey>,
    txn: HashMap<RelationId, Vec<DecodedChange>>,
    end_lsn: u64,
    commit_time_micros: i64,
) {
    let commit_time = client::pg_epoch_to_system_time(commit_time_micros);

    for (relation_id, changes) in txn {
        if changes.is_empty() {
            continue;
        }
        let Some(member_key) = routes.get(&relation_id) else {
            continue;
        };
        let Some(member) = source.member(member_key) else {
            continue; // detached mid-transaction
        };
        let Some(rel) = decoder.relation(relation_id) else {
            member_fatal(
                source,
                member_key,
                format!(
                    "change event before Relation for id {relation_id} \
                     (dataset {})",
                    member.dataset_name
                ),
            )
            .await;
            continue;
        };
        let batch = match super::changes::build_change_batch(&member.schema, rel, &changes) {
            Ok(b) => b,
            Err(e) => {
                member_fatal(
                    source,
                    member_key,
                    format!("change batch build failed for {}: {e}", member.dataset_name),
                )
                .await;
                continue;
            }
        };
        member.metrics.inc_transaction();
        // Readiness was already signaled by the member's snapshot / ready
        // envelope at subscribe time, so WAL envelopes never need to carry it.
        let envelope = ChangeEnvelope::new(
            Box::new(SharedLsnCommitter {
                ack: Arc::clone(&source.ack),
                key: member_key.clone(),
                flush_to: end_lsn,
            }),
            batch,
            false,
        );
        source.ack.deliver(member_key, end_lsn);
        if member.sender.send(Ok(envelope)).await.is_err() {
            source.detach_member(member_key, "changes stream receiver dropped");
        }
    }

    // Slot-level freshness signal: every live member has now seen WAL through
    // this commit (routed members via their envelope, others by exclusion).
    source.for_each_member_metrics(|m| m.record_commit_watermark(commit_time));
    source.ack.credit_idle(end_lsn);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> MemberKey {
        ("public".to_string(), s.to_string())
    }

    #[test]
    fn ack_floor_is_min_across_members() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"));
        ack.register(&key("b"));

        ack.deliver(&key("a"), 200);
        ack.commit(&key("a"), 200);
        // b has neither delivered nor committed past 100 → floor stays at 100.
        assert_eq!(ack.flush_lsn(), 100);

        ack.deliver(&key("b"), 200);
        ack.commit(&key("b"), 200);
        assert_eq!(ack.flush_lsn(), 200);
    }

    #[test]
    fn ack_credit_idle_skips_members_with_inflight_envelopes() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"));
        ack.register(&key("b"));

        // a has an in-flight envelope (delivered past committed).
        ack.deliver(&key("a"), 300);
        ack.credit_idle(500);

        // b (idle) was credited to 500; a holds the floor at its committed 100.
        assert_eq!(ack.flush_lsn(), 100);
        ack.commit(&key("a"), 300);
        assert_eq!(ack.flush_lsn(), 300);
        // Now a is idle again — a later credit advances everyone.
        ack.credit_idle(600);
        assert_eq!(ack.flush_lsn(), 600);
    }

    #[test]
    fn detached_member_holds_the_floor() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"));
        ack.register(&key("b"));
        ack.credit_idle(200);
        assert_eq!(ack.flush_lsn(), 200);

        ack.detach(&key("a"));
        // Detached member is never credited: floor pins at its last commit.
        ack.credit_idle(900);
        assert_eq!(ack.flush_lsn(), 200);

        // Rejoin keeps the held floor; once it commits the replayed tail, the
        // floor moves again.
        ack.register(&key("a"));
        ack.deliver(&key("a"), 900);
        ack.commit(&key("a"), 900);
        ack.credit_idle(900);
        assert_eq!(ack.flush_lsn(), 900);
    }

    #[test]
    fn replayed_older_commits_never_regress() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"));
        ack.deliver(&key("a"), 500);
        ack.commit(&key("a"), 500);
        assert_eq!(ack.flush_lsn(), 500);

        // A replayed envelope (post-reconnect) carries an older LSN.
        ack.deliver(&key("a"), 300);
        ack.commit(&key("a"), 300);
        assert_eq!(ack.flush_lsn(), 500);
        // And the member still counts as idle for crediting.
        ack.credit_idle(700);
        assert_eq!(ack.flush_lsn(), 700);
    }

    #[tokio::test]
    async fn shared_committer_advances_member_floor() {
        let ack = Arc::new(AckTable::default());
        ack.seed(10);
        ack.register(&key("a"));
        let committer = SharedLsnCommitter {
            ack: Arc::clone(&ack),
            key: key("a"),
            flush_to: 42,
        };
        assert!(committer.supports_deferral());
        committer.commit().await.expect("commit");
        assert_eq!(ack.flush_lsn(), 42);
    }

    #[test]
    fn source_key_distinguishes_connections_and_slots() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let base = SourceKey {
            host: "h".into(),
            port: 5432,
            database: "db".into(),
            user: "u".into(),
            slot_name: "slot".into(),
        };
        let other_slot = SourceKey {
            slot_name: "slot2".into(),
            ..base.clone()
        };
        assert_ne!(base, other_slot);
        let mut h1 = DefaultHasher::new();
        base.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        base.clone().hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }
}
