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

    /// Detach a member: stop routing to it but hold its ack floor so the slot
    /// never acknowledges past what it durably applied. See the module docs
    /// for why the hold (and the WAL retention it causes) is intentional.
    ///
    /// A member that detaches while its initial snapshot was still running has
    /// an accelerator missing base rows that WAL replay can never provide, so
    /// its table is (best-effort) removed from the publication — any rejoin,
    /// in-process or after a restart, then re-adds the table and takes a fresh
    /// snapshot.
    fn detach_member(&self, key: &MemberKey, reason: &str) {
        let removed = lock(&self.members).remove(key);
        let was_snapshotting = self.ack.detach(key);
        lock(&self.detached).insert(key.clone());
        if let Some(member) = removed {
            tracing::warn!(
                dataset = %member.dataset_name,
                table = %format_member(key),
                slot = %self.key.slot_name,
                reason,
                was_snapshotting,
                "shared replication member detached; its last applied LSN now pins WAL \
                 retention for the shared slot until the dataset rejoins or spiced restarts \
                 (watch dataset_postgres_replication_lag_bytes)"
            );
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
            source.detach_member(&member_key, "superseded by a new subscription");
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
    let (sender, receiver) = mpsc::channel(MEMBER_CHANNEL_CAPACITY);
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
    source.detach_member(key, "fatal member error");
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
            if !source.ack.is_streaming(&member_key) {
                // The member is still held — snapshotting, or joined after
                // this connection started. Don't route WAL at it (a
                // snapshotting member's channel isn't drained until the
                // snapshot ends). Its held ack floor keeps this WAL
                // replayable; the next (re)connect promotes it and re-sends
                // this Relation.
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
            routes.insert(rel.relation_id, member_key);
        }
        DecodedMessage::Insert { relation_id, tuple } => {
            if let Some(member_key) = routes.get(&relation_id)
                && let Some(member) = source.member(member_key)
            {
                member.metrics.inc_insert();
                txn.entry(relation_id).or_default().push(DecodedChange {
                    op: ChangeOp::Create,
                    row: tuple,
                });
            }
        }
        DecodedMessage::Update {
            relation_id,
            old,
            new,
        } => {
            if let Some(member_key) = routes.get(&relation_id)
                && let Some(member) = source.member(member_key)
            {
                member.metrics.inc_update();
                // Fill unchanged-TOAST markers from the old tuple (REPLICA
                // IDENTITY FULL) before buffering.
                let new = super::changes::merge_unchanged_toast(new, old.as_ref());
                txn.entry(relation_id).or_default().push(DecodedChange {
                    op: ChangeOp::Update,
                    row: new,
                });
            }
        }
        DecodedMessage::Delete { relation_id, old } => {
            if let Some(member_key) = routes.get(&relation_id)
                && let Some(member) = source.member(member_key)
            {
                member.metrics.inc_delete();
                txn.entry(relation_id).or_default().push(DecodedChange {
                    op: ChangeOp::Delete,
                    row: old,
                });
            }
        }
        DecodedMessage::Truncate { relation_ids } => {
            // Unlike the per-dataset path, multi-relation TRUNCATEs are fine
            // here: each relation routes to its own member.
            for relation_id in relation_ids {
                if let Some(member_key) = routes.get(&relation_id)
                    && let Some(member) = source.member(member_key)
                {
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
