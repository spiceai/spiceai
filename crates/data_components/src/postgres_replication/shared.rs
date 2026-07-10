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
//! While a member snapshots, the pump routes nothing at it (so a long
//! snapshot never back-pressures the other members through the bounded
//! channel); its ack floor is held at the join LSN instead. On clean snapshot
//! completion the pump reconnects from the held floor and *promotes* the
//! member at connect time — only a connection that provably starts at or
//! below a member's floor makes it routable and creditable, which closes the
//! window where changes decoded before the reconnect could be acknowledged
//! past an unrouted member. A snapshot that fails leaves an accelerator
//! missing base rows WAL can never provide, so a mid-snapshot detach removes
//! the table from the publication (best-effort), forcing any rejoin back
//! through the fresh-snapshot path.
//!
//! # Ack / WAL-retention model
//!
//! `confirmed_flush_lsn` is per-slot, so the shared slot acknowledges the
//! *minimum* durably-applied LSN across all members ([`AckTable`]). Members
//! with no traffic are credited forward on keepalives/commits whenever they
//! have no in-flight envelopes. A stalled or failed member therefore pins WAL
//! retention for the whole slot **by design** — acking past it would lose its
//! changes permanently. The detached state is observable directly via the
//! `dataset_postgres_replication_member_attached` gauge (1 attached / 0 detached)
//! and an ERROR log on a stalling detach; the resulting WAL growth also shows on
//! `dataset_postgres_replication_lag_bytes` (which, on a shared slot, grows for
//! the *surviving* members whose ack floor is pinned by the detached one — the
//! member gauge is the unambiguous signal for *which* dataset stalled).
//! Restarting spiced (or the member rejoining) heals it by replaying from the
//! held LSN, which every member applies idempotently.
//!
//! # Backpressure vs. server liveness
//!
//! A slow member sink backpressures the pump: `deliver_commit` blocks on the
//! member channel, which stops the pump calling `client.recv()`, which lets
//! events pile up in the `pgwire_replication` worker's channel. What keeps this
//! from killing the connection is the **worker** (`pgwire_replication`'s
//! `send_event`): it keeps emitting standby status feedback on `status_interval`
//! while its own channel is full, so Postgres never hits `wal_sender_timeout`.
//! The pump-side handling here ([`MEMBER_SEND_STALL_WARN`], the `send_timeout`
//! loop in `deliver_commit`) is therefore *not* what prevents server-side
//! timeout — it exists only for observability (the
//! `dataset_postgres_replication_member_send_stalled_seconds_total` counter) and
//! to keep the pump's shutdown check responsive while a member stalls.
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
use pgwire_replication::{Lsn, ReplicationClient, ReplicationEvent, TryRecvEvent};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use bytes::Bytes;

use super::{
    Error, ReplicationMetricsCollector, ReplicationStreamInput, Result, bootstrap,
    changes::PgChangeRows, client, config::ReplicationParams, pgoutput, resilience, slot,
};
use rustc_hash::FxHashMap;

use crate::cdc::{ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError};
use crate::postgres_replication::pgoutput::RelationId;

/// Per-connection routing table: relation id -> (member key, resolved handle).
/// `FxHashMap` (not the `SipHash` default) since the keys are trusted internal
/// relation ids and this is on the per-event pump hot path — a `u32` `FxHash`
/// is ~1-2ns vs `SipHash`'s ~10-20ns, and bit-mixing keeps `hashbrown`'s SIMD
/// filter effective (unlike an identity `nohash`, whose zero high bits defeat
/// it as the map grows).
type RouteMap = FxHashMap<RelationId, (MemberKey, Arc<MemberHandle>)>;

/// Per-transaction buffer of raw pgoutput change bytes, keyed by relation id.
/// `FxHashMap` for the same hot-path reason as [`RouteMap`].
type TxnBuffer = FxHashMap<RelationId, Vec<Bytes>>;

/// Default bounded per-member delivery queue depth (envelopes), overridable via
/// `pg_replication_member_channel_capacity`
/// ([`ReplicationParams::member_channel_capacity`]). When one member's sink
/// stops draining the pump blocks on its channel and the whole shared stream
/// pauses (in addition to the WAL pinning the ack floor already causes) —
/// bounded memory is preferred over unbounded buffering behind a stalled sink.
/// This queue sits in front of the accelerator's much larger prefetch buffer,
/// so a too-shallow value turns a member's transient stall into slot-wide
/// head-of-line blocking; the default is deep enough to absorb a burst without
/// transmitting one member's stall to the whole slot.
pub const DEFAULT_MEMBER_CHANNEL_CAPACITY: usize = 1024;

/// Upper bound on how long the pump blocks in `recv()` before re-checking
/// membership (joins, dropped receivers). Idle Postgres servers can go tens
/// of seconds between messages; this keeps membership changes responsive.
const RECV_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Yield to the Tokio scheduler after draining this many buffered events via the
/// non-blocking `try_recv` fast path. Most `handle_decoded` branches (Insert /
/// Update / Delete during a transaction) never reach a real `.await`, so a large
/// buffered transaction would otherwise be processed in a tight loop that never
/// yields — starving other tasks on the worker (including `/health`). The
/// blocking `recv()` path participates in Tokio's cooperative budget and needs
/// no explicit yield; only the sync `try_recv` drain does. Matches Tokio's own
/// coop budget (128) so the cadence is unchanged from the pre-`try_recv` loop.
const DRAIN_YIELD_INTERVAL: usize = 128;

/// How long a single member's committed-change delivery may block the pump
/// before we emit a WARN, bump the stall metric, and re-check for shutdown.
/// Server-side liveness is *not* at risk here — the `pgwire_replication` worker
/// keeps sending standby status feedback while its own channel backs up (see
/// its `send_event`); this bound is purely for observability and to keep the
/// pump's shutdown check responsive while one member's sink is slow.
const MEMBER_SEND_STALL_WARN: std::time::Duration = std::time::Duration::from_secs(5);

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
    /// `GENERATED` columns of the member's source table — absent from
    /// pgoutput `Relation` messages by Postgres design; tolerated during
    /// schema validation and applied as NULL.
    generated_columns: Vec<String>,
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
    /// `true` while the member's initial snapshot runs. Cleared by the
    /// snapshot-completion hook; a member that detaches with this still set
    /// has an accelerator missing base rows.
    snapshotting: bool,
    /// `true` once the pump has (re)connected at or below this member's floor
    /// — only then is the member routed and creditable. Members are *held*
    /// (no routing, never credited, floor pinned) from registration until
    /// that promotion: crediting a member before a connection provably covers
    /// its gap would acknowledge WAL it never received (changes decoded while
    /// it had no route), losing them permanently.
    streaming: bool,
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
    /// Register a member (or revive a detached one) in the *held* state. A
    /// rejoining member keeps its held `committed` floor — everything after
    /// it is about to be replayed.
    fn register(&self, key: &MemberKey, snapshotting: bool) {
        let at = self.flush_lsn();
        let mut entries = lock(&self.entries);
        entries
            .entry(key.clone())
            .and_modify(|e| {
                e.live = true;
                e.delivered = e.committed;
                e.snapshotting = snapshotting;
                e.streaming = false;
            })
            .or_insert(AckEntry {
                committed: at,
                delivered: at,
                live: true,
                snapshotting,
                streaming: false,
            });
    }

    /// The member's initial snapshot finished cleanly. It stays *held* until
    /// the pump's next (re)connect promotes it — the caller must also request
    /// that reconnect.
    fn snapshot_finished(&self, key: &MemberKey) {
        let mut entries = lock(&self.entries);
        if let Some(e) = entries.get_mut(key) {
            e.snapshotting = false;
        }
    }

    /// Called by the pump immediately after a successful connect, whose
    /// `start_lsn` was the floor (min over all `committed`, held members
    /// included): every held, snapshot-complete member's gap is covered by
    /// this connection's replay, so they become routable and creditable.
    fn promote_ready_members(&self) {
        let mut entries = lock(&self.entries);
        for e in entries.values_mut() {
            if e.live && !e.snapshotting {
                e.streaming = true;
            }
        }
    }

    fn is_streaming(&self, key: &MemberKey) -> bool {
        lock(&self.entries).get(key).is_some_and(|e| e.streaming)
    }

    /// Whether the member has already durably applied this commit — used to
    /// suppress re-delivery of envelopes during a reconnect replay (the replay
    /// always starts at the *minimum* floor, so caught-up members would
    /// otherwise see every commit since the slowest member's position again).
    fn already_committed(&self, key: &MemberKey, lsn: u64) -> bool {
        lock(&self.entries)
            .get(key)
            .is_some_and(|e| e.committed >= lsn)
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

    /// Credit streaming members with no in-flight envelopes up to `upto` —
    /// the connection's in-order replay guarantees their routed changes below
    /// `upto` were already delivered. Detached entries are never credited
    /// (that's the point of the hold), and neither are held (not-yet-promoted)
    /// members — they have no route yet, so "no in-flight envelopes" says
    /// nothing about what they've missed.
    fn credit_idle(&self, upto: u64) {
        {
            let mut entries = lock(&self.entries);
            for e in entries.values_mut() {
                if e.live && e.streaming && e.delivered == e.committed {
                    let lsn = e.committed.max(upto);
                    e.committed = lsn;
                    e.delivered = lsn;
                }
            }
        }
        self.recompute();
    }

    /// Detach a member, returning whether it was still snapshotting (its
    /// snapshot never completed cleanly).
    fn detach(&self, key: &MemberKey) -> bool {
        let mut entries = lock(&self.entries);
        match entries.get_mut(key) {
            Some(e) => {
                e.live = false;
                e.streaming = false;
                e.snapshotting
            }
            None => false,
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

    /// Publish one boundary's accumulated metrics to every live member in a
    /// single pass. Replaces the several separate `for_each_member_metrics`
    /// walks a commit/keepalive used to do (reader timing + `confirmed_flush` +
    /// commit watermark) — one `live_members()` snapshot, one iteration. Skips
    /// the members lock entirely when there is nothing to report.
    fn flush_member_metrics(&self, b: &BoundaryMetrics) {
        if b.input_wait_us == 0
            && b.processing_us == 0
            && b.server_wal_end == 0
            && b.confirmed_flush_lsn == 0
            && b.commit_watermark.is_none()
        {
            return;
        }
        self.for_each_member_metrics(|m| {
            if b.input_wait_us > 0 {
                m.add_reader_input_wait_micros(b.input_wait_us);
            }
            if b.processing_us > 0 {
                m.add_reader_processing_micros(b.processing_us);
            }
            if b.server_wal_end > 0 {
                m.set_server_wal_end(b.server_wal_end);
            }
            if b.confirmed_flush_lsn > 0 {
                m.set_confirmed_flush_lsn(b.confirmed_flush_lsn);
            }
            if let Some(at) = b.commit_watermark {
                m.record_commit_watermark(at);
            }
        });
    }

    /// Detach a member: stop routing to it but hold its ack floor so the slot
    /// never acknowledges past what it durably applied. See the module docs
    /// for why the hold (and the WAL retention it causes) is intentional.
    ///
    /// A member that detaches while its initial snapshot was still running has
    /// an accelerator missing base rows that WAL replay can never provide, so
    /// its table is (best-effort) removed from the publication — any rejoin,
    /// in-process or after a restart, then re-adds the table and takes a fresh
    /// snapshot.
    /// Detach a member from the shared slot. `stalls_slot` distinguishes a
    /// genuine, unhealed stall (the member's changes stream died and its ack
    /// floor now pins WAL for every slot-mate until it rejoins or spiced
    /// restarts — a page-worthy, ERROR-level condition) from a self-healing
    /// supersede (an already-closed member being replaced by an incoming
    /// re-subscription, which re-attaches immediately — only WARN).
    fn detach_member(&self, key: &MemberKey, reason: &str, stalls_slot: bool) {
        let removed = lock(&self.members).remove(key);
        let was_snapshotting = self.ack.detach(key);
        lock(&self.detached).insert(key.clone());
        if let Some(member) = removed {
            // Flip the membership-liveness gauge to detached (0) so the state is
            // observable, not only logged (#11644). A superseding re-subscription
            // re-attaches (back to 1) via `mark_member_attached` on rejoin.
            member.metrics.mark_member_detached();
            if stalls_slot {
                tracing::error!(
                    dataset = %member.dataset_name,
                    table = %format_member(key),
                    slot = %self.key.slot_name,
                    reason,
                    was_snapshotting,
                    "shared replication member detached; its last applied LSN now pins WAL \
                     retention for the shared slot until the dataset rejoins or spiced restarts \
                     (watch dataset_postgres_replication_member_attached and \
                     dataset_postgres_replication_lag_bytes)"
                );
            } else {
                tracing::warn!(
                    dataset = %member.dataset_name,
                    table = %format_member(key),
                    slot = %self.key.slot_name,
                    reason,
                    was_snapshotting,
                    "shared replication member detached and is being replaced by a new \
                     subscription (rejoin in progress)"
                );
            }
        }
        if was_snapshotting {
            let params = self.params.clone();
            let (schema_name, table_name) = key.clone();
            let slot_name = self.key.slot_name.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    slot::remove_table_from_publication(&params, &schema_name, &table_name).await
                {
                    tracing::warn!(
                        table = %format!("{schema_name}.{table_name}"),
                        slot = %slot_name,
                        "failed to remove mid-snapshot table from the shared publication; \
                         re-adding the dataset will resume WITHOUT a fresh snapshot — drop \
                         the table from the publication manually before re-adding: {e}"
                    );
                }
            });
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
            self.detach_member(&key, "changes stream receiver dropped", true);
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
    if let Some(existing) = registry.get(key)
        && !existing.dead.load(Ordering::Acquire)
    {
        return Arc::clone(existing);
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

    // The pump serves every member over ONE connection built from the first
    // subscriber's params. Reject members whose connection-level settings
    // differ rather than silently using someone else's — a mismatched
    // sslmode, for example, would otherwise quietly downgrade (or break) the
    // transport this dataset asked for. (host/port/database/user are already
    // part of the registry key; member-level knobs like the snapshot batch
    // size stay per-dataset.)
    if let Some(param) = connection_params_mismatch(&params, &source.params) {
        return Err(Error::SharedConnectionParamsMismatch {
            dataset: dataset_name,
            param,
        });
    }

    if let Some(existing) = source.member(&member_key) {
        if existing.sender.is_closed() {
            // The previous subscription's receiver is gone (dataset reload,
            // failed sink) but the pump hasn't reaped it yet — detach it now
            // so this is a rejoin, not a duplicate.
            source.detach_member(&member_key, "superseded by a new subscription", false);
        } else {
            return Err(Error::SharedTableAlreadySubscribed {
                schema: schema_name,
                table: table_name,
                slot: source.key.slot_name.clone(),
            });
        }
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
    // table newly added to the publication (late-added dataset, or a rejoin
    // after a failed snapshot — mid-bootstrap detach removes the table from
    // the publication), or a slot created fresh this process (regardless of
    // leftover publication membership). A plain rejoin skips it — the held
    // ack floor guarantees the gap is still in WAL and will be replayed.
    //
    // `snapshot_on_resume` overrides all of that: a non-persistent
    // accelerator starts empty every boot, so WAL replay alone can never
    // reconstruct it — snapshot-then-replay is the only correct sequence.
    let need_snapshot = params.snapshot_on_resume
        || setup.table_added
        || (!rejoining && source.slot_created_fresh.load(Ordering::Acquire));

    let snapshotting = need_snapshot && params.initial_snapshot;
    let (sender, receiver) = mpsc::channel(params.member_channel_capacity);
    // Grouping signal for the analysis: record which shared slot this dataset joined.
    // (Membership liveness is marked by `mark_member_attached` just below.)
    metrics.set_slot_name(source.key.slot_name.clone());
    source.ack.register(&member_key, snapshotting);
    lock(&source.members).insert(
        member_key.clone(),
        Arc::new(MemberHandle {
            dataset_name: dataset_name.clone(),
            schema: Arc::clone(&schema),
            primary_keys: primary_keys.clone(),
            generated_columns: setup.generated_columns.clone(),
            sender,
            metrics: Arc::clone(&metrics),
        }),
    );
    // Membership liveness is now observable (`dataset_postgres_replication_member_attached`):
    // this dataset is an attached member of the shared slot. Covers both a fresh
    // join and an in-process rejoin (both reach here); the paired `mark_member_detached`
    // in `detach_member` flips it to 0 when the member leaves (#11644).
    metrics.mark_member_attached();

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
    if !setup.generated_columns.is_empty() {
        tracing::warn!(
            dataset = %dataset_name,
            columns = ?setup.generated_columns,
            "source table has GENERATED column(s): Postgres does not publish generated \
             columns over logical replication, so they are populated by the initial \
             snapshot but will be NULL on replicated changes. Exclude them from the \
             dataset schema if NULLs are unacceptable."
        );
    }

    if !source.pump_started.swap(true, Ordering::AcqRel) {
        let pump_source = Arc::clone(source);
        tokio::spawn(run_pump(pump_source));
    } else if !snapshotting {
        // A resuming/rejoining member needs the pump to reconnect so Postgres
        // re-sends Relation messages (and replays its held WAL) — joins
        // coalesce; the flag is cleared once per reconnect. A snapshotting
        // member does NOT: it stays held until its snapshot completes, and
        // the completion hook requests the reconnect then. Skipping the
        // join-time reconnect also keeps a crash-looping member (e.g. a
        // snapshot that keeps failing on an unsupported column) from forcing
        // a reconnect storm on the healthy members.
        source.restart_requested.store(true, Ordering::Release);
    }

    // Head of the member's stream: initial snapshot (its last envelope flips
    // is_dataset_ready), or an immediate ready signal when resuming.
    //
    // While the snapshot runs, the pump does NOT route WAL to this member
    // (its `bootstrapping` ack entry both signals that and holds the join-LSN
    // floor), so one member's long snapshot never back-pressures the others
    // through its undrained channel. When the snapshot completes, the
    // `bootstrap_finished` hook flips the member live and asks the pump to
    // reconnect — Postgres resumes from the held floor and replays the
    // member's gap (idempotent for everyone).
    let head: ChangesStream = if snapshotting {
        let snapshot = bootstrap::snapshot_stream(bootstrap::SnapshotInput {
            params: params.clone(),
            schema_name,
            table_name,
            dataset_schema: Arc::clone(&schema),
            primary_keys,
            dataset_name,
            metrics,
        })?;
        // Flip the member live only on CLEAN snapshot completion. If the
        // snapshot errored, the member must stay `bootstrapping`: its
        // accelerator is missing base rows that WAL replay can never provide,
        // so the eventual detach tears its table out of the publication and a
        // rejoin re-snapshots from scratch.
        let saw_error = Arc::new(AtomicBool::new(false));
        let error_flag = Arc::clone(&saw_error);
        let snapshot = snapshot.inspect(move |item| {
            if item.is_err() {
                error_flag.store(true, Ordering::Release);
            }
        });
        let hook_source = Arc::clone(source);
        let hook_key = member_key.clone();
        let mut hook_fired = false;
        let bootstrap_finished = stream::poll_fn(move |_| {
            if !hook_fired {
                hook_fired = true;
                if !saw_error.load(Ordering::Acquire) {
                    hook_source.ack.snapshot_finished(&hook_key);
                    hook_source.restart_requested.store(true, Ordering::Release);
                }
            }
            std::task::Poll::Ready(None)
        });
        Box::pin(snapshot.chain(bootstrap_finished))
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

/// Compare connection-level params of a joining member against the shared
/// source's. Returns the name of the first mismatched parameter, never its
/// value — passwords and certificate paths must not leak into error messages.
fn connection_params_mismatch(
    member: &ReplicationParams,
    source: &ReplicationParams,
) -> Option<&'static str> {
    use secrecy::ExposeSecret;
    if member.password.expose_secret() != source.password.expose_secret() {
        return Some("pg_pass");
    }
    if member.sslmode != source.sslmode {
        return Some("pg_sslmode");
    }
    if member.sslrootcert != source.sslrootcert {
        return Some("pg_sslrootcert");
    }
    if member.temporary_slot != source.temporary_slot {
        return Some("pg_replication_temporary_slot");
    }
    None
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
    source.detach_member(key, "fatal member error", true);
}

/// Mark the source dead and drop it from the registry (only if the registry
/// still points at this instance — a replacement may already exist).
fn finish_pump(source: &Arc<SharedSource>) {
    source.dead.store(true, Ordering::Release);
    let mut registry = lock(&REGISTRY);
    if let Some(current) = registry.get(&source.key)
        && Arc::ptr_eq(current, source)
    {
        registry.remove(&source.key);
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
/// Per-boundary metric snapshot fanned out to every live member in a single
/// pass (see [`SharedSource::flush_member_metrics`]). #11610 already moved the
/// reader-timing fan-out off the per-`XLogData` path onto commit/keepalive
/// boundaries; this collapses the *several* separate `for_each_member_metrics`
/// walks a boundary still did (reader timing, `confirmed_flush`, commit
/// watermark) into one members-lock + one iteration. All fields are optional:
/// an idle tick reports only the reader timing, a keepalive adds
/// `confirmed_flush`, and a commit adds the watermark too.
#[derive(Default)]
struct BoundaryMetrics {
    /// Microseconds blocked awaiting the source socket since the last flush.
    input_wait_us: u64,
    /// Microseconds spent decoding/routing since the last flush, with member
    /// send-wait already subtracted (see `deliver_commit`).
    processing_us: u64,
    /// Running max of the server's reported WAL end (monotonic; exact to flush).
    server_wal_end: u64,
    /// Shared ack floor to publish as each member's `confirmed_flush_lsn`, or
    /// `0` to leave it unchanged (idle ticks do not advance the ack).
    confirmed_flush_lsn: u64,
    /// Commit time of the transaction just delivered, for the freshness
    /// watermark; `None` on keepalive/idle boundaries.
    commit_watermark: Option<std::time::SystemTime>,
}

/// Outcome of one acquisition step in the pump's recv loop. Unifies the
/// non-blocking `try_recv` fast path and the timed blocking `recv` so their
/// error handling lives in exactly one place.
enum Acquired {
    /// An event to process.
    Event(ReplicationEvent),
    /// The poll timed out with the buffer empty — flush accumulated timing and
    /// re-check membership.
    Idle,
    /// The server closed the stream cleanly; reconnect.
    CleanClose,
    /// A recv error (transient → reconnect, else fatal broadcast).
    RecvError(pgwire_replication::PgWireError),
}

async fn run_pump(source: Arc<SharedSource>) {
    // Captured at pump start: the pump stops when the epoch advances (this
    // Runtime began shutting down); a pump started by a later Runtime in the
    // same process captures the newer epoch and is unaffected.
    let shutdown_epoch = crate::cdc::shutdown_epoch();
    let params = source.params.clone();
    let slot_name = source.key.slot_name.clone();
    let publication_name = params.publication_name.clone();
    let mut backoff = resilience::Backoff::default_for_stream();
    let mut reconnect_attempts: u32 = 0;
    // When the stream dropped (set as the inner loop breaks to reconnect); consumed
    // on the next successful connect to attribute the disconnected duration.
    let mut disconnect_at: Option<std::time::Instant> = None;

    'reconnect: loop {
        if crate::cdc::shutdown_epoch() != shutdown_epoch {
            tracing::info!(
                slot = %slot_name,
                "runtime shutdown; releasing shared replication connection and slot"
            );
            finish_pump(&source);
            return;
        }
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
                // Attribute the disconnected duration (drop → resume) to every member.
                if let Some(dropped_at) = disconnect_at.take() {
                    let down_ms =
                        u64::try_from(dropped_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                    source.for_each_member_metrics(|m| m.add_disconnected_ms(down_ms));
                }
                if reconnect_attempts > 0 {
                    tracing::info!(
                        slot = %slot_name,
                        attempts = reconnect_attempts,
                        "shared replication connection resumed"
                    );
                    reconnect_attempts = 0;
                }
                // This connection starts at the floor (min over every held
                // member), so it covers every snapshot-complete member's gap:
                // promote them to routable + creditable.
                source.ack.promote_ready_members();
                c
            }
            Err(e) if resilience::is_transient_pgwire(&e) => {
                // Mark the outage start on the first failed attempt so a boot-time /
                // never-yet-connected outage (no prior success set `disconnect_at`) still
                // contributes to `replication_disconnected_ms_total`. `get_or_insert_with`
                // preserves an earlier drop timestamp (an established connection that fell
                // over then failed to reconnect), so the full outage span is attributed.
                disconnect_at.get_or_insert_with(std::time::Instant::now);
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
        // Relation id -> (member key, resolved handle). Caching the `Arc<MemberHandle>`
        // here (once, at Relation time) keeps the per-event route lookup a bare
        // `u32` hash — no `members` mutex, no `(String, String)` hash on the hot path.
        let mut routes: RouteMap = RouteMap::default();
        // Raw pgoutput change-message bytes, buffered per relation as the pump
        // routes them (no tuple decode on this shared task). The per-dataset
        // consumer decodes + builds them (see `PgChangeRows`).
        let mut txn: TxnBuffer = TxnBuffer::default();
        let mut txn_open = false;

        // Reader-timing accumulators (see `BoundaryMetrics` /
        // `flush_member_metrics`): summed per decoded event, fanned out to
        // members once per commit/keepalive/idle tick rather than per row. Reset
        // per connection; the partial tail on a mid-txn reconnect is a negligible
        // loss for a diagnostic metric.
        let mut input_us_acc: u64 = 0;
        let mut proc_us_acc: u64 = 0;
        let mut max_wal_end: u64 = 0;
        // Set on a Commit boundary so the consolidated flush also refreshes the
        // freshness watermark; cleared on each flush.
        let mut commit_watermark: Option<std::time::SystemTime> = None;
        // Buffered events processed via the non-blocking `try_recv` fast path
        // since the last cooperative yield (see `DRAIN_YIELD_INTERVAL`).
        let mut drained_since_yield: usize = 0;

        'recv: loop {
            if crate::cdc::shutdown_epoch() != shutdown_epoch {
                // Drop the client now (releasing the walsender + slot) rather
                // than at process exit — the graceful-shutdown drain phase
                // can hold the process alive for tens of seconds.
                tracing::info!(
                    slot = %slot_name,
                    "runtime shutdown; releasing shared replication connection and slot"
                );
                drop(client);
                finish_pump(&source);
                return;
            }
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

            // Acquire the next event. Fast path: drain events the worker has
            // already buffered via the non-blocking `try_recv`, which arms no
            // timer — so the per-message `timeout(..)` cost is paid once per
            // idle gap, not once per event. Only when the buffer is empty (or
            // the worker closed) do we block on the timed `recv()`; that bounds
            // the wait so membership changes (joins via the restart flag,
            // receiver drops via the reap above) are noticed within
            // ~`RECV_POLL_INTERVAL` even on a quiet server.
            //
            // A `try_recv` hit adds no input-wait (there was nothing to wait
            // for); only the blocking wait feeds `input_us_acc`. That keeps the
            // input-wait vs. processing split (the source-bound vs. our-decode
            // discriminator) honest. Both `recv`/`try_recv` read an internal
            // channel and are cancel-safe.
            let mut should_flush = false;
            let acquired = match client.try_recv() {
                Ok(TryRecvEvent::Event(e)) => {
                    drained_since_yield += 1;
                    Acquired::Event(e)
                }
                Err(e) => Acquired::RecvError(e),
                // Buffer drained (or worker gone): block for the next event.
                // `Closed` falls here too so the blocking `recv()` reaps the
                // worker's terminal `Ok(None)`/`Err`. The blocking `recv()`
                // participates in Tokio's cooperative budget, so reaching here
                // is itself a yield opportunity — reset the drain counter.
                Ok(TryRecvEvent::Empty | TryRecvEvent::Closed) => {
                    drained_since_yield = 0;
                    let recv_start = std::time::Instant::now();
                    let polled = tokio::time::timeout(RECV_POLL_INTERVAL, client.recv()).await;
                    input_us_acc = input_us_acc.saturating_add(
                        u64::try_from(recv_start.elapsed().as_micros()).unwrap_or(u64::MAX),
                    );
                    match polled {
                        Err(_elapsed) => Acquired::Idle,
                        Ok(Ok(Some(e))) => Acquired::Event(e),
                        // Server closed cleanly (e.g. orderly Postgres shutdown):
                        // treat like a transient drop and reconnect — the shared
                        // stream is meant to run for the process lifetime.
                        Ok(Ok(None)) => Acquired::CleanClose,
                        Ok(Err(e)) => Acquired::RecvError(e),
                    }
                }
            };
            let event = match acquired {
                Acquired::Event(e) => e,
                Acquired::Idle => {
                    // Idle tick: flush the accumulated reader timing so idle
                    // time is attributed even when no server event arrives for a
                    // while. No ack/watermark change on an idle boundary.
                    source.flush_member_metrics(&BoundaryMetrics {
                        input_wait_us: input_us_acc,
                        processing_us: proc_us_acc,
                        server_wal_end: max_wal_end,
                        ..BoundaryMetrics::default()
                    });
                    input_us_acc = 0;
                    proc_us_acc = 0;
                    max_wal_end = 0;
                    continue 'recv;
                }
                Acquired::CleanClose => break 'recv,
                Acquired::RecvError(e) => {
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

            // Stay cooperative while draining a long run of buffered events off
            // the sync `try_recv` fast path: most `handle_decoded` branches never
            // reach a real `.await`, so a large buffered transaction would
            // otherwise monopolize this worker thread and starve other tasks
            // (including `/health`). Yield every `DRAIN_YIELD_INTERVAL` events;
            // the blocking `recv()` path already resets the counter to 0.
            if drained_since_yield >= DRAIN_YIELD_INTERVAL {
                drained_since_yield = 0;
                tokio::task::yield_now().await;
            }

            let processing_start = std::time::Instant::now();
            // Microseconds spent blocked delivering this event's committed
            // changes into slow member channels (set only by a Commit). Kept
            // separate so it can be subtracted from processing below —
            // downstream back-pressure is not our decode cost.
            let mut send_wait_us: u64 = 0;
            match event {
                ReplicationEvent::Begin { .. } => {
                    txn_open = true;
                    txn.clear();
                }
                ReplicationEvent::XLogData { data, wal_end, .. } => {
                    max_wal_end = max_wal_end.max(wal_end.0);
                    // Peek the message type to route WITHOUT decoding the tuple:
                    // Relation/Truncate are fully decoded here (rare, and they
                    // carry routing state — the relation cache / a relation-id
                    // list); Insert/Update/Delete are only peeked for their
                    // relation id and buffered raw, so the per-dataset consumer
                    // pays the tuple decode + Arrow build off this shared task.
                    match pgoutput::message_type(&data) {
                        // Relation and Truncate are fully decoded here (rare, and
                        // they carry routing state — the relation cache / a
                        // relation-id list). Clone the frame first so a TRUNCATE
                        // can still buffer its raw bytes after the decoder consumes
                        // `data` (O(1) Bytes refcount; R/T are rare).
                        Some(b'R' | b'T') => {
                            let raw = data.clone();
                            let msg = match decoder.decode(data) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    source.for_each_member_metrics(
                                        ReplicationMetricsCollector::inc_decode_error,
                                    );
                                    fatal_broadcast(
                                        &source,
                                        format!("pgoutput decode failed: {e}"),
                                    )
                                    .await;
                                    break 'reconnect;
                                }
                            };
                            match msg {
                                pgoutput::DecodedMessage::Relation(rel) => {
                                    handle_relation(&source, &mut decoder, &mut routes, rel).await;
                                }
                                pgoutput::DecodedMessage::Truncate { relation_ids } => {
                                    buffer_raw_truncate(&routes, &mut txn, &relation_ids, &raw);
                                }
                                // A non-R/T body under an R/T tag is impossible
                                // from a well-formed server; ignore.
                                _ => {}
                            }
                        }
                        // Insert/Update/Delete: peek the relation id to route +
                        // meter, then buffer the raw bytes; the per-dataset
                        // consumer pays the tuple decode + Arrow build off this
                        // shared task.
                        Some(tag @ (b'I' | b'U' | b'D')) => {
                            buffer_raw_change(&routes, &mut txn, tag, data);
                        }
                        // Type / Origin / Message / Stream* — safe to ignore.
                        _ => {}
                    }
                }
                ReplicationEvent::Commit {
                    end_lsn,
                    commit_time_micros,
                    ..
                } => {
                    txn_open = false;
                    send_wait_us = deliver_commit(
                        &source,
                        &decoder,
                        &routes,
                        std::mem::take(&mut txn),
                        end_lsn.0,
                        commit_time_micros,
                        shutdown_epoch,
                    )
                    .await;
                    // The ack floor + freshness watermark are published to every
                    // member by the single consolidated boundary flush below, not
                    // a separate per-member pass.
                    commit_watermark = Some(client::pg_epoch_to_system_time(commit_time_micros));
                    client.update_applied_lsn(Lsn(source.ack.flush_lsn()));
                    should_flush = true;
                }
                ReplicationEvent::KeepAlive { wal_end, .. } => {
                    // Accumulate the server WAL end; the per-member fan-out
                    // (server_wal_end + confirmed_flush) happens once in the
                    // consolidated boundary flush below, not inline per event.
                    max_wal_end = max_wal_end.max(wal_end.0);
                    should_flush = true;
                    if !txn_open {
                        source.ack.credit_idle(wal_end.0);
                    }
                    client.update_applied_lsn(Lsn(source.ack.flush_lsn()));
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
            // Processing (decode + route) for this event, paired with the
            // input-wait above. Accumulated locally; fanned out to members only
            // on a commit/keepalive boundary (`should_flush`) — the frequent
            // per-row XLogData events just accumulate, keeping the hot decode
            // path free of the per-event member-iteration fan-out.
            //
            // `send_wait_us` — time the Commit spent BLOCKED `await`ing a slow
            // member's bounded channel in `deliver_commit` — is subtracted here:
            // that is downstream back-pressure, not our decode cost, and is
            // carried per dataset by `member_send_wait_micros_total` instead. So
            // the reader-processing bucket stays honest (decode + route only) and
            // the classifier no longer reads READER-decode-bound when the truth
            // is apply-bound. (Resolves the earlier reader-split caveat.)
            let processed_us =
                u64::try_from(processing_start.elapsed().as_micros()).unwrap_or(u64::MAX);
            proc_us_acc = proc_us_acc.saturating_add(processed_us.saturating_sub(send_wait_us));
            // Flush on a commit/keepalive boundary, or periodically (~1s of
            // accumulated wait+processing) so a long transaction still reports
            // before its commit. A periodic flush carries no commit watermark
            // (commit_watermark stays None) and does not call `credit_idle`, but
            // it may still publish a higher confirmed_flush if members advanced
            // the shared ack floor asynchronously.
            let us_since_flush = input_us_acc.saturating_add(proc_us_acc);
            if should_flush || us_since_flush >= 1_000_000 {
                source.flush_member_metrics(&BoundaryMetrics {
                    input_wait_us: input_us_acc,
                    processing_us: proc_us_acc,
                    server_wal_end: max_wal_end,
                    confirmed_flush_lsn: source.ack.flush_lsn(),
                    commit_watermark,
                });
                input_us_acc = 0;
                proc_us_acc = 0;
                max_wal_end = 0;
                commit_watermark = None;
            }
        } // end 'recv

        // Inner loop broke for reconnect (transient error or membership
        // change). On membership change the backoff was just reset by the
        // successful connect, so the wait is the minimal initial delay.
        // Mark the drop so the next successful connect can attribute the
        // disconnected duration (this wait + reconnect handshake).
        disconnect_at = Some(std::time::Instant::now());
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

/// Validate a (re)decoded Relation against its subscribed dataset and (re)build
/// its route. Members that are held (snapshotting, or joined after this
/// connection started) are left unrouted until the next reconnect promotes
/// them. Runs on the pump — Relations are rare (once per (slot, relation) and
/// on schema change).
async fn handle_relation(
    source: &Arc<SharedSource>,
    decoder: &mut pgoutput::Decoder,
    routes: &mut RouteMap,
    rel: pgoutput::Relation,
) {
    let member_key: MemberKey = (rel.namespace.clone(), rel.name.clone());
    let Some(member) = source.member(&member_key) else {
        // No member for this table (e.g. publication membership left over from
        // a removed dataset). Its changes are dropped (never routed).
        routes.remove(&rel.relation_id);
        tracing::debug!(
            table = %format_member(&member_key),
            slot = %source.key.slot_name,
            "relation in shared publication has no subscribed dataset; ignoring its changes"
        );
        return;
    };
    if !source.ack.is_streaming(&member_key) {
        // The member is still held — snapshotting, or joined after this
        // connection started. Don't route WAL at it (a snapshotting member's
        // channel isn't drained until the snapshot ends). Its held ack floor
        // keeps this WAL replayable; the next (re)connect promotes it and
        // re-sends this Relation.
        routes.remove(&rel.relation_id);
        tracing::debug!(
            dataset = %member.dataset_name,
            table = %format_member(&member_key),
            "member is not yet streaming; deferring WAL routing until the next reconnect"
        );
        return;
    }
    if let Err(e) = client::validate_relation_against_schema(
        &member.schema,
        &rel,
        &member.primary_keys,
        &member.generated_columns,
    ) {
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
    // Cache the resolved handle alongside the key so the per-event path skips
    // the `members` lock + string hash (see `buffer_raw_change`).
    routes.insert(rel.relation_id, (member_key, member));
}

/// Route an Insert/Update/Delete by its peeked relation id and buffer the raw
/// pgoutput bytes for the per-dataset consumer to decode. The tuple is NOT
/// decoded here — only the relation id (routing) and `tag` (per-op metric) are
/// read. A change for a relation with no streaming member is dropped, matching
/// the eager path. The "change before Relation" invariant is still enforced at
/// commit (`deliver_commit` fatals if the relation isn't cached).
fn buffer_raw_change(routes: &RouteMap, txn: &mut TxnBuffer, tag: u8, data: Bytes) {
    let Some(relation_id) = pgoutput::relation_id(&data) else {
        return;
    };
    // Cached at Relation time: a `u32` lookup yields the member handle directly,
    // so the per-event hot path takes no `members` lock and hashes no strings.
    let Some((_member_key, member)) = routes.get(&relation_id) else {
        return;
    };
    match tag {
        b'I' => member.metrics.inc_insert(),
        b'U' => member.metrics.inc_update(),
        b'D' => member.metrics.inc_delete(),
        _ => {}
    }
    txn.entry(relation_id).or_default().push(data);
}

/// Route a Truncate to every subscribed relation in its id list and buffer the
/// raw bytes per relation. Unlike the per-dataset path, multi-relation
/// TRUNCATEs are fine here: each relation routes to its own member.
fn buffer_raw_truncate(
    routes: &RouteMap,
    txn: &mut TxnBuffer,
    relation_ids: &[RelationId],
    data: &Bytes,
) {
    for &relation_id in relation_ids {
        if let Some((_member_key, member)) = routes.get(&relation_id) {
            member.metrics.inc_truncate();
            txn.entry(relation_id).or_default().push(data.clone());
            tracing::info!(
                dataset = %member.dataset_name,
                relation_id,
                "TRUNCATE from shared postgres replication queued for accelerator"
            );
        }
    }
}

/// Outcome of delivering one envelope into a member's channel. Every variant
/// carries the microseconds spent `await`ing the channel (already recorded into
/// the member's `member_send_wait_micros_total`), which the caller folds into
/// the commit total it subtracts from reader-processing.
enum SendOutcome {
    /// Delivered.
    Sent(u64),
    /// The member's receiver was dropped — the caller should detach it.
    ReceiverGone(u64),
    /// The runtime began shutting down mid-wait — the caller should abandon the
    /// rest of the commit and let the pump release the slot.
    ShutdownAbandon(u64),
}

/// Must-deliver one envelope into a member's bounded channel, timing the wait.
///
/// The envelope carries committed changes and a `SharedLsnCommitter` that
/// advances the ack floor, so it cannot be dropped under back-pressure. But one
/// slow member must not block the pump (and thus every other member)
/// indefinitely or wedge shutdown, so the wait is bounded: on each
/// `MEMBER_SEND_STALL_WARN` tick we WARN + bump the stall metric, and abandon if
/// the runtime is shutting down. Server-side liveness is handled a layer down by
/// the worker. Records the full awaited time into `member_send_wait_micros_total`
/// (≈0 when the channel had spare capacity) and returns it in the outcome.
async fn deliver_to_member(
    metrics: &ReplicationMetricsCollector,
    dataset_name: &str,
    sender: &mpsc::Sender<std::result::Result<ChangeEnvelope, StreamError>>,
    envelope: std::result::Result<ChangeEnvelope, StreamError>,
    shutdown_epoch: u64,
) -> SendOutcome {
    let send_start = std::time::Instant::now();
    let waited =
        |start: std::time::Instant| u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
    let mut pending = envelope;
    loop {
        match sender.send_timeout(pending, MEMBER_SEND_STALL_WARN).await {
            Ok(()) => {
                let w = waited(send_start);
                metrics.add_member_send_wait_micros(w);
                return SendOutcome::Sent(w);
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                let w = waited(send_start);
                metrics.add_member_send_wait_micros(w);
                return SendOutcome::ReceiverGone(w);
            }
            Err(mpsc::error::SendTimeoutError::Timeout(returned)) => {
                if crate::cdc::shutdown_epoch() != shutdown_epoch {
                    let w = waited(send_start);
                    metrics.add_member_send_wait_micros(w);
                    return SendOutcome::ShutdownAbandon(w);
                }
                metrics.add_send_stalled(MEMBER_SEND_STALL_WARN.as_secs());
                tracing::warn!(
                    dataset = %dataset_name,
                    stalled_for = ?MEMBER_SEND_STALL_WARN,
                    "shared Postgres CDC member sink is not draining; the pump is \
                     waiting to deliver committed changes (watch \
                     dataset_postgres_replication_member_send_stalled_seconds_total)"
                );
                pending = returned;
            }
        }
    }
}

/// Route a committed transaction's buffered changes to their members, then
/// credit idle members and recompute the shared ack floor. Returns the total
/// microseconds spent `await`ing slow member channels during this commit — the
/// caller subtracts it from the reader-processing accumulator so downstream
/// back-pressure is not misattributed to decode cost (it is carried per dataset
/// by `member_send_wait_micros_total`). Returns ~0 whenever every member's
/// channel had spare capacity.
async fn deliver_commit(
    source: &Arc<SharedSource>,
    decoder: &pgoutput::Decoder,
    routes: &RouteMap,
    txn: TxnBuffer,
    end_lsn: u64,
    commit_time_micros: i64,
    shutdown_epoch: u64,
) -> u64 {
    let commit_time = client::pg_epoch_to_system_time(commit_time_micros);
    // Unix-epoch ms for the per-batch replication-lag signal carried into the
    // accelerator (distinct from the `SystemTime` watermark published by the
    // caller's boundary flush).
    let commit_ts_ms = commit_time
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok());
    // Total time blocked awaiting member channels this commit, returned to the
    // caller for the reader-processing subtraction.
    let mut total_send_wait_us: u64 = 0;

    for (relation_id, raw) in txn {
        if raw.is_empty() {
            continue;
        }
        // The handle is the one cached at Relation time. A member can detach
        // mid-connection (via `member_fatal` or a re-subscribe) while its route
        // entry lingers until the next reconnect rebuilds `routes`, so re-check
        // it's still streaming before delivering. Skipping this would keep
        // routing committed changes to a detached member and — because
        // `AckTable::commit` doesn't check `live` — advance its ack floor past
        // its last applied LSN, breaking the "stop routing but hold the floor"
        // detach contract (and delivering changes after a fatal error). This is
        // a per-commit gate, not the per-event hot path, so the lookup is fine.
        let Some((member_key, member)) = routes.get(&relation_id) else {
            continue;
        };
        if !source.ack.is_streaming(member_key) {
            continue;
        }
        if source.ack.already_committed(member_key, end_lsn) {
            // Reconnect replay of a commit this member already durably
            // applied (replays start at the minimum floor across members).
            continue;
        }
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
        // Defer the entire tuple decode + O(rows x columns) Arrow-typing/UTF-8
        // build off this shared pump task onto the per-dataset consumer: the
        // pump only peeked + buffered the raw bytes, and `PgChangeRows::build`
        // decodes + builds later on the consumer (see `ChangeRows`), so neither
        // decode nor build serializes every member behind one thread. The
        // relation is cloned once per commit-per-relation (schema metadata, not
        // per row) so the rows own their inputs; a decode/build failure (e.g. an
        // unmergeable unchanged-TOAST column) then surfaces as a `StreamError` on
        // this dataset's stream at consume time rather than a pump-side
        // `member_fatal`, isolating it to the one dataset.
        let rows = PgChangeRows::new(Arc::clone(&member.schema), rel.clone(), raw, commit_ts_ms);
        member.metrics.inc_transaction();
        // Readiness was already signaled by the member's snapshot / ready
        // envelope at subscribe time, so WAL envelopes never need to carry it.
        let envelope = ChangeEnvelope::new_from_rows(
            Box::new(SharedLsnCommitter {
                ack: Arc::clone(&source.ack),
                key: member_key.clone(),
                flush_to: end_lsn,
            }),
            Box::new(rows),
            false,
        );
        source.ack.deliver(member_key, end_lsn);
        match deliver_to_member(
            &member.metrics,
            &member.dataset_name,
            &member.sender,
            Ok(envelope),
            shutdown_epoch,
        )
        .await
        {
            SendOutcome::Sent(waited) => {
                total_send_wait_us = total_send_wait_us.saturating_add(waited);
            }
            SendOutcome::ReceiverGone(waited) => {
                total_send_wait_us = total_send_wait_us.saturating_add(waited);
                source.detach_member(member_key, "changes stream receiver dropped", true);
            }
            SendOutcome::ShutdownAbandon(waited) => {
                return total_send_wait_us.saturating_add(waited);
            }
        }
    }

    // The slot-level freshness watermark for this commit is published to every
    // member by the caller's consolidated boundary flush
    // (`BoundaryMetrics::commit_watermark`), not a separate per-member pass.
    source.ack.credit_idle(end_lsn);

    total_send_wait_us
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> MemberKey {
        ("public".to_string(), s.to_string())
    }

    /// A one-column schema — `build_ready_signal_envelope` cannot build a
    /// zero-field struct array, so tests that emit a ready envelope need at
    /// least one field.
    fn tiny_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        ]))
    }

    /// Minimal shared params for tests that only exercise members/metrics; the
    /// connection fields are never dialed.
    fn test_params() -> ReplicationParams {
        ReplicationParams {
            host: "localhost".to_string(),
            port: 5432,
            user: "u".to_string(),
            password: secrecy::SecretString::from(String::new()),
            database: "db".to_string(),
            sslmode: crate::postgres_replication::config::SslMode::Disable,
            sslrootcert: None,
            slot_name: "slot".to_string(),
            publication_name: "pub".to_string(),
            initial_snapshot: true,
            snapshot_on_resume: false,
            temporary_slot: false,
            status_interval: std::time::Duration::from_secs(5),
            bootstrap_batch_size: 8192,
            shared: true,
            member_channel_capacity: DEFAULT_MEMBER_CHANNEL_CAPACITY,
            pg_output_format: crate::postgres_replication::PgOutputFormat::Binary,
        }
    }

    type MemberProbe = (
        MemberKey,
        Arc<ReplicationMetricsCollector>,
        mpsc::Receiver<std::result::Result<ChangeEnvelope, StreamError>>,
    );

    /// Build a `SharedSource` with `n` members wired to fresh metrics collectors
    /// and (capacity-4) channels, returning a probe per member so tests can read
    /// its metrics and drive its channel.
    fn test_source_with_members(n: usize) -> (Arc<SharedSource>, Vec<MemberProbe>) {
        let source_key = SourceKey::from_params(&test_params());
        let source = Arc::new(SharedSource::new(source_key, test_params()));
        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
        let mut probes = Vec::with_capacity(n);
        for i in 0..n {
            let member_key = key(&format!("t{i}"));
            let metrics = ReplicationMetricsCollector::new();
            let (sender, receiver) = mpsc::channel(4);
            lock(&source.members).insert(
                member_key.clone(),
                Arc::new(MemberHandle {
                    dataset_name: format!("ds{i}"),
                    schema: Arc::clone(&schema),
                    primary_keys: vec![],
                    generated_columns: vec![],
                    sender,
                    metrics: Arc::clone(&metrics),
                }),
            );
            probes.push((member_key, metrics, receiver));
        }
        (source, probes)
    }

    /// Item 1: one boundary flush publishes every field to every member.
    #[test]
    fn flush_member_metrics_fans_all_fields_to_every_member() {
        let (source, probes) = test_source_with_members(3);
        let watermark =
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        source.flush_member_metrics(&BoundaryMetrics {
            input_wait_us: 11,
            processing_us: 22,
            server_wal_end: 500,
            confirmed_flush_lsn: 400,
            commit_watermark: Some(watermark),
        });
        for (_, collector, _rx) in &probes {
            let m = crate::postgres_replication::ReplicationMetrics::new(Arc::clone(collector));
            assert_eq!(m.reader_input_wait_micros_total(), 11);
            assert_eq!(m.reader_processing_micros_total(), 22);
            assert_eq!(m.server_wal_end_lsn(), 500);
            assert_eq!(m.confirmed_flush_lsn(), 400);
            assert!(
                m.replication_lag_ms().is_some(),
                "watermark should set lag_ms"
            );
        }
    }

    /// Item 1: an all-zero boundary touches nothing (the members-lock fast path).
    #[test]
    fn flush_member_metrics_empty_boundary_is_a_noop() {
        let (source, probes) = test_source_with_members(2);
        source.flush_member_metrics(&BoundaryMetrics::default());
        for (_, collector, _rx) in &probes {
            let m = crate::postgres_replication::ReplicationMetrics::new(Arc::clone(collector));
            assert_eq!(m.reader_input_wait_micros_total(), 0);
            assert_eq!(m.reader_processing_micros_total(), 0);
            assert_eq!(m.server_wal_end_lsn(), 0);
            assert_eq!(m.confirmed_flush_lsn(), 0);
        }
    }

    /// Item 4: a stalled (full) member channel lands the pump's blocked time in
    /// `member_send_wait_micros_total` — the value the caller subtracts from the
    /// reader-processing bucket — while a channel with spare capacity waits ~0.
    #[tokio::test]
    async fn deliver_to_member_backpressure_lands_in_send_wait() {
        let epoch = crate::cdc::shutdown_epoch();
        let schema = tiny_schema();
        let collector = ReplicationMetricsCollector::new();
        let (tx, mut rx) = mpsc::channel::<std::result::Result<ChangeEnvelope, StreamError>>(1);

        // Spare capacity → immediate send, negligible wait.
        let env0 = Ok(crate::cdc::build_ready_signal_envelope(&schema).expect("ready envelope"));
        match deliver_to_member(&collector, "ds", &tx, env0, epoch).await {
            SendOutcome::Sent(w) => assert!(w < 100_000, "fast path should be ~0µs, got {w}"),
            _ => panic!("expected Sent on the fast path"),
        }
        let _ = rx.recv().await.expect("drain fast-path envelope");

        // Fill to capacity; free a slot only after a delay so the next send blocks.
        tx.send(Ok(
            crate::cdc::build_ready_signal_envelope(&schema).expect("env")
        ))
        .await
        .expect("prefill to capacity");
        let drainer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(60)).await;
            let _ = rx.recv().await; // free one slot
            rx // keep the receiver alive until the send completes
        });
        let env1 = Ok(crate::cdc::build_ready_signal_envelope(&schema).expect("env"));
        let SendOutcome::Sent(waited) = deliver_to_member(&collector, "ds", &tx, env1, epoch).await
        else {
            panic!("expected Sent once the slot frees")
        };
        assert!(
            waited >= 40_000,
            "expected ≥40ms blocked wait, got {waited}µs"
        );

        let m = crate::postgres_replication::ReplicationMetrics::new(Arc::clone(&collector));
        assert!(
            m.member_send_wait_micros_total() >= waited,
            "member_send_wait counter must include the blocked wait"
        );
        let _rx = drainer.await.expect("drainer task");
    }

    /// Item 4: a dropped receiver is reported so the caller detaches the member.
    #[tokio::test]
    async fn deliver_to_member_reports_receiver_gone() {
        let epoch = crate::cdc::shutdown_epoch();
        let schema = tiny_schema();
        let collector = ReplicationMetricsCollector::new();
        let (tx, rx) = mpsc::channel::<std::result::Result<ChangeEnvelope, StreamError>>(1);
        drop(rx);
        let env = Ok(crate::cdc::build_ready_signal_envelope(&schema).expect("env"));
        assert!(matches!(
            deliver_to_member(&collector, "ds", &tx, env, epoch).await,
            SendOutcome::ReceiverGone(_)
        ));
    }

    #[test]
    fn ack_floor_is_min_across_members() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"), false);
        ack.register(&key("b"), false);

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
        ack.register(&key("a"), false);
        ack.register(&key("b"), false);
        ack.promote_ready_members();

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
    fn held_member_is_never_credited_until_promoted() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"), true);
        ack.register(&key("b"), false);
        ack.promote_ready_members();
        assert!(
            !ack.is_streaming(&key("a")),
            "a snapshotting → not promoted"
        );
        assert!(ack.is_streaming(&key("b")));

        // Commits flow for b, but a's join-LSN floor holds while it snapshots.
        ack.deliver(&key("b"), 300);
        ack.commit(&key("b"), 300);
        ack.credit_idle(300);
        assert_eq!(ack.flush_lsn(), 100);

        // Snapshot finished — but a stays held (and uncredited) until the
        // pump's next connect promotes it. Crediting in this window would ack
        // past changes decoded while a had no route (the data-loss race).
        ack.snapshot_finished(&key("a"));
        ack.credit_idle(400);
        assert_eq!(ack.flush_lsn(), 100);
        assert!(!ack.is_streaming(&key("a")));

        // The reconnect (started at the floor, covering a's gap) promotes a.
        ack.promote_ready_members();
        assert!(ack.is_streaming(&key("a")));
        ack.credit_idle(400);
        assert_eq!(ack.flush_lsn(), 400);
    }

    #[test]
    fn detach_reports_snapshotting_state() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"), true);
        ack.register(&key("b"), false);
        assert!(ack.detach(&key("a")), "a detached mid-snapshot");
        assert!(!ack.detach(&key("b")), "b was not snapshotting");
        assert!(
            !ack.detach(&key("zz")),
            "unknown member is not snapshotting"
        );
    }

    #[test]
    fn detached_member_holds_the_floor() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"), false);
        ack.register(&key("b"), false);
        ack.promote_ready_members();
        ack.credit_idle(200);
        assert_eq!(ack.flush_lsn(), 200);

        ack.detach(&key("a"));
        // Detached member is never credited: floor pins at its last commit.
        ack.credit_idle(900);
        assert_eq!(ack.flush_lsn(), 200);

        // Rejoin keeps the held floor; once promoted by the reconnect and the
        // replayed tail commits, the floor moves again.
        ack.register(&key("a"), false);
        ack.promote_ready_members();
        ack.deliver(&key("a"), 900);
        ack.commit(&key("a"), 900);
        ack.credit_idle(900);
        assert_eq!(ack.flush_lsn(), 900);
    }

    #[test]
    fn replayed_older_commits_never_regress() {
        let ack = AckTable::default();
        ack.seed(100);
        ack.register(&key("a"), false);
        ack.promote_ready_members();
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
        ack.register(&key("a"), false);
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
