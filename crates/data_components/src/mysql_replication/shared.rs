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

//! Share one `MySQL` binlog dump connection across multiple
//! `refresh_mode: changes` datasets — the `MySQL` analog of
//! [`crate::postgres_replication::shared`].
//!
//! Unlike a Postgres publication, `MySQL`'s `COM_BINLOG_DUMP` has no
//! server-side table filter: every subscriber receives the *entire* server
//! binlog. So the per-dataset path opens one full-server dump per table and
//! discards every other table's events, and N datasets cost N dumps + N
//! `server_id`s. Sharing is implicit-by-group: datasets on the same
//! `(host, port, user, database)` naming the same `mysql_replication_group`
//! join a single *shared source* — one dump connection, one `server_id`, with
//! decoded transactions routed by `(database, table)` to each member's
//! accelerator sink. A group named by only one dataset degenerates to
//! per-dataset behavior (single member). Datasets without a group keep their
//! dedicated per-dataset stream.
//!
//! # Consistency & ack model
//!
//! Identical at-least-once contract to the per-dataset path, made convergent by
//! PK-based upsert. `MySQL` keeps no server-side cursor, so there is no slot to
//! acknowledge: instead every member persists its *own* committed
//! [`BinlogPosition`] into its own `spice_sys_mysql_binlog` sidecar row, and the
//! shared dump's resume position is the **minimum** committed position across
//! all members ([`AckTable::flush_position`]). On (re)start the pump resumes
//! from that min; members ahead of it replay idempotently.
//!
//! A member takes an initial snapshot when it has no usable persisted position
//! (cold, or an incompatible/purged checkpoint resolved by
//! `mysql_replication_invalid_checkpoint_behavior`). While a member snapshots
//! it is *held* — the pump routes nothing at it and its floor is pinned at its
//! join head — so a long snapshot never back-pressures the other members. On
//! clean snapshot completion the pump reconnects from the held min and
//! *promotes* every snapshot-complete member; a connection that provably starts
//! at/below a member's floor makes it routable, and members already ahead
//! suppress the replay via [`AckSlot::already_committed`].
//!
//! # WAL-retention caveat (differs from Postgres)
//!
//! A Postgres slot pins server-side WAL retention, so a stalled member's held
//! floor is safe indefinitely. `MySQL` has **no such pin**: a detached member's
//! held floor pins only the *shared resume position*, and if the source purges
//! binlogs past it (`binlog_expire_logs_seconds`) the whole group must
//! re-bootstrap. This is observable — the
//! `dataset_mysql_replication_member_attached` gauge flips to 0 and an ERROR is
//! logged on detach — and recovery is bounded by
//! `mysql_replication_invalid_checkpoint_behavior`, applied per member on the
//! next resume. There is no slowness heuristic: a slow-but-live member is
//! handled by channel backpressure, never detached.
//!
//! # v1 scope: schema drift
//!
//! A DDL statement classified as a schema change (ALTER/DROP/RENAME) is
//! member-fatal — that one member detaches, the group keeps running. A
//! column-count change first observed at a `TableMap` (a DDL the tokenizer did
//! not classify) is adopted only if the freshly-fetched source layout matches
//! the event's column count ([`super::binlog::adopt_current_layout`]), else
//! member-fatal; the count-match guard makes replaying pre-change row images
//! self-fatal rather than mis-decoded. The per-dataset `Checkpointer`'s durable
//! pre-adopt/replay-boundary machinery is intentionally *not* ported. Instead,
//! safety across a detach/rejoin is guaranteed structurally: every (re)subscribe
//! re-resolves the start position from the member's own sidecar and re-checks
//! its persisted layout fingerprint against the current source layout, so a
//! schema change that happened while the member was gone triggers
//! `invalid_checkpoint_behavior` (rebootstrap/error) rather than replaying old
//! row images under the new layout. A snapshotting member's head is never
//! persisted until its snapshot is durably applied (a boundary committer, not a
//! stream-drain hook), so a crash mid-snapshot re-snapshots — never resumes past
//! un-applied base rows.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use mysql_async::Conn;
use mysql_async::binlog::events::{EventData, RowsEventData, TableMapEvent};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::binlog::{
    AdoptedLayout, MIN_VALID_EVENT_POS, QueryKind, StatementKind, adopt_current_layout,
    classify_query, classify_statement, commit_ts_ms, compute_pk_source_indexes,
    log_transient_reconnect, open_binlog_stream, purged_position_error, readiness_heartbeat,
    record_watermark,
};
use super::changes::{MemberLayout, MysqlChangeRows};
use super::config::{BinlogPosition, ReplicationParams};
use super::metrics::MetricsCollector;
use super::rows::{build_change_batch, truncate_change};
use super::{
    Error, PersistedPosition, PositionStore, ReplicationStreamInput, Result,
    check_resume_compatibility, encode_checkpoint_schema_json, stream_error,
};
use crate::cdc::{ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError};
use crate::cdc::{InitialSnapshotMode, InvalidCheckpointBehavior};

/// Default bounded per-member delivery queue depth (envelopes). When one
/// member's sink stops draining, the pump blocks on its channel and the whole
/// shared stream pauses — bounded memory is preferred over unbounded buffering
/// behind a stalled sink. Deep enough to absorb a burst without transmitting a
/// transient stall to the whole group.
const DEFAULT_MEMBER_CHANNEL_CAPACITY: usize = 1024;

/// Upper bound on how long the pump waits for the next binlog event before
/// re-checking membership, shutdown, and readiness. Keeps a quiet source
/// responsive to joins/detaches and idle checkpointing.
const IDLE_TICK_CAP: Duration = Duration::from_secs(1);

/// How long a single member's delivery may block the pump before we WARN and
/// re-check shutdown. Purely observability + shutdown responsiveness; server
/// liveness is not at risk (the dump connection keeps its own keepalives).
const MEMBER_SEND_STALL_WARN: Duration = Duration::from_secs(5);

/// `(database, table)` of a member's source table — the routing key.
type MemberKey = (String, String);

/// Identity of a shared dump. Datasets whose connection params and group
/// produce the same key share one pump.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct SourceKey {
    host: String,
    port: u16,
    user: String,
    database: String,
    group: String,
}

impl SourceKey {
    fn from_params(params: &ReplicationParams) -> Self {
        Self {
            host: params.opts.ip_or_hostname().to_string(),
            port: params.opts.tcp_port(),
            user: params.opts.user().unwrap_or_default().to_string(),
            database: params.opts.db_name().unwrap_or_default().to_string(),
            group: params.group.clone().unwrap_or_default(),
        }
    }
}

static REGISTRY: LazyLock<Mutex<HashMap<SourceKey, Arc<SharedSource>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Poison-shrugging lock helpers: every critical section here is a short
/// read-modify-write over plain data, so a panicking peer cannot leave the map
/// logically broken.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}
fn read_lock<T>(m: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(PoisonError::into_inner)
}
fn write_lock<T>(m: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(PoisonError::into_inner)
}

/// `LIVE`: attached; routing to it is allowed once also `STREAMING`.
const LIVE: u8 = 0b001;
/// `SNAPSHOTTING`: the member's initial snapshot is still running.
const SNAPSHOTTING: u8 = 0b010;
/// `STREAMING`: the pump has (re)connected at/below this member's floor, so it
/// is routable and creditable. Members are held (no routing, floor pinned)
/// from registration until this promotion.
const STREAMING: u8 = 0b100;

/// Per-member position accounting. Unlike the Postgres `AckSlot` (a lock-free
/// `AtomicU64` LSN), a [`BinlogPosition`] is `(file, pos)` and not atomically
/// representable, so `committed`/`delivered` sit behind a short-held `Mutex`.
/// This is fine: the binlog commit rate is orders of magnitude below Postgres's
/// per-LSN churn and each slot's mutex is uncontended (one committer + the
/// pump), so the guard is held only for a monotonic compare-and-set.
struct AckSlot {
    committed: Mutex<BinlogPosition>,
    delivered: Mutex<BinlogPosition>,
    state: AtomicU8,
}

impl AckSlot {
    fn new(at: BinlogPosition, snapshotting: bool) -> Self {
        Self {
            committed: Mutex::new(at.clone()),
            delivered: Mutex::new(at),
            state: AtomicU8::new(LIVE | if snapshotting { SNAPSHOTTING } else { 0 }),
        }
    }

    fn committed(&self) -> BinlogPosition {
        lock(&self.committed).clone()
    }
    fn delivered(&self) -> BinlogPosition {
        lock(&self.delivered).clone()
    }

    /// Advance this member's committed floor (monotonic-max).
    fn commit(&self, to: &BinlogPosition) {
        let mut g = lock(&self.committed);
        if *to > *g {
            *g = to.clone();
        }
    }
    /// Record an envelope delivered into this member's channel (monotonic-max).
    fn deliver(&self, to: &BinlogPosition) {
        let mut g = lock(&self.delivered);
        if *to > *g {
            *g = to.clone();
        }
    }

    /// Whether the member has already durably applied this commit — used to
    /// suppress re-delivery during a reconnect replay from the shared min.
    fn already_committed(&self, at: &BinlogPosition) -> bool {
        *lock(&self.committed) >= *at
    }

    fn has(&self, flag: u8) -> bool {
        self.state.load(Ordering::Acquire) & flag != 0
    }
}

/// Per-member position accounting for a shared dump. The shared resume position
/// is the minimum `committed` over **all** members (held members included —
/// their frozen floor pins the resume by design), recomputed lazily on read.
#[derive(Default)]
struct AckTable {
    members: RwLock<HashMap<MemberKey, Arc<AckSlot>>>,
}

impl AckTable {
    /// Register a member (or revive a detached one) in the *held* state at the
    /// caller-resolved floor `at`. Unlike Postgres (whose server slot is the
    /// source of truth, so a rejoin preserves its in-memory floor), `MySQL`'s
    /// source of truth is the per-member sidecar: `attach_member` re-resolves
    /// `at` from it on every (re)subscribe (layout-checked), so a reviving slot
    /// is RESET to `at` — a resume position, or a fresh head on rebootstrap.
    /// `at` may be below the stale held floor; the pump replays the gap
    /// idempotently. Held from here until a (re)connect promotes it.
    fn register(&self, key: &MemberKey, at: BinlogPosition, snapshotting: bool) {
        let held = LIVE | if snapshotting { SNAPSHOTTING } else { 0 };
        let mut members = write_lock(&self.members);
        match members.get(key) {
            Some(slot) => {
                *lock(&slot.committed) = at.clone();
                *lock(&slot.delivered) = at;
                slot.state.store(held, Ordering::Release);
            }
            None => {
                members.insert(key.clone(), Arc::new(AckSlot::new(at, snapshotting)));
            }
        }
    }

    /// The member's initial snapshot finished cleanly; it stays held until the
    /// next (re)connect promotes it (the caller requests that reconnect).
    fn snapshot_finished(&self, key: &MemberKey) {
        if let Some(slot) = write_lock(&self.members).get(key) {
            slot.state.fetch_and(!SNAPSHOTTING, Ordering::AcqRel);
        }
    }

    /// After a successful connect from the shared min: every held,
    /// snapshot-complete member's gap is covered by this connection's replay, so
    /// promote them to routable + creditable.
    fn promote_ready_members(&self) {
        for slot in write_lock(&self.members).values() {
            let s = slot.state.load(Ordering::Acquire);
            if s & LIVE != 0 && s & SNAPSHOTTING == 0 {
                slot.state.fetch_or(STREAMING, Ordering::AcqRel);
            }
        }
    }

    fn slot(&self, key: &MemberKey) -> Option<Arc<AckSlot>> {
        read_lock(&self.members).get(key).map(Arc::clone)
    }

    /// Credit streaming members with no in-flight envelopes up to `upto` — the
    /// connection's in-order replay guarantees their routed changes below `upto`
    /// were already delivered. Held/detached members are never credited.
    fn credit_idle(&self, upto: &BinlogPosition) {
        for slot in read_lock(&self.members).values() {
            let s = slot.state.load(Ordering::Acquire);
            if s & (LIVE | STREAMING) == (LIVE | STREAMING) && slot.delivered() == slot.committed()
            {
                slot.commit(upto);
                slot.deliver(upto);
            }
        }
    }

    /// Detach a member, returning whether it was still snapshotting. The slot
    /// stays in the map with its `committed` frozen as a held floor.
    fn detach(&self, key: &MemberKey) -> bool {
        match write_lock(&self.members).get(key) {
            Some(slot) => {
                let prev = slot.state.fetch_and(!(LIVE | STREAMING), Ordering::AcqRel);
                prev & SNAPSHOTTING != 0
            }
            None => false,
        }
    }

    /// The shared resume position: the minimum `committed` over all members
    /// (held members included). `None` when there are no members.
    fn flush_position(&self) -> Option<BinlogPosition> {
        read_lock(&self.members)
            .values()
            .map(|slot| slot.committed())
            .min()
    }

    fn is_streaming(&self, key: &MemberKey) -> bool {
        self.slot(key).is_some_and(|slot| slot.has(STREAMING))
    }

    #[cfg(test)]
    fn committed(&self, key: &MemberKey) -> Option<BinlogPosition> {
        self.slot(key).map(|s| s.committed())
    }

    #[cfg(test)]
    fn delivered(&self, key: &MemberKey) -> Option<BinlogPosition> {
        self.slot(key).map(|s| s.delivered())
    }
}

/// `CommitChange` for shared-dump envelopes: advances this member's committed
/// position via its own [`AckSlot`]. Persistence to the sidecar is batched by
/// the pump on the checkpoint interval (see [`run_pump`]), so a commit stays as
/// cheap as the per-dataset in-memory ack.
struct SharedPositionCommitter {
    slot: Arc<AckSlot>,
    flush_to: BinlogPosition,
    dataset: String,
    source_commit_ts_ms: Option<i64>,
}

#[async_trait]
impl CommitChange for SharedPositionCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.slot.commit(&self.flush_to);
        crate::cdc::log_committer_progress(
            "mysql",
            &self.dataset,
            &self.flush_to.to_string(),
            self.source_commit_ts_ms,
        );
        Ok(())
    }

    /// The shared min holds the resume until this member's floor advances past
    /// this commit, so a crash before a deferred commit re-streams the un-acked
    /// tail (idempotent). Safe to defer.
    fn supports_deferral(&self) -> bool {
        true
    }

    /// Fold a later commit to the *same* slot by keeping the higher position —
    /// sound because [`SharedPositionCommitter::commit`] is a monotonic-max.
    fn try_absorb(&mut self, other: &dyn CommitChange) -> bool {
        match other
            .as_any()
            .and_then(|a| a.downcast_ref::<SharedPositionCommitter>())
        {
            Some(other) if Arc::ptr_eq(&self.slot, &other.slot) => {
                if other.flush_to > self.flush_to {
                    self.flush_to = other.flush_to.clone();
                    self.source_commit_ts_ms = other.source_commit_ts_ms;
                }
                true
            }
            _ => false,
        }
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

/// Committer chained after a member's initial snapshot. Its `commit()` runs on
/// the apply loop only after every snapshot batch is durably applied (in-order
/// commit), so it is the barrier at which the member becomes safe to promote
/// and persist. It clears `SNAPSHOTTING` and asks the pump to reconnect (which
/// promotes the member from the shared min). Carries no data — a zero-row
/// boundary batch — and must not defer (it has to run to fire the promotion).
struct SnapshotBoundaryCommitter {
    source: Arc<SharedSource>,
    key: MemberKey,
    dataset: String,
}

#[async_trait]
impl CommitChange for SnapshotBoundaryCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.source.ack.snapshot_finished(&self.key);
        self.source.restart_requested.store(true, Ordering::Release);
        crate::cdc::log_committer_progress("mysql", &self.dataset, "snapshot-complete", None);
        Ok(())
    }
}

struct MemberHandle {
    dataset_name: String,
    schema: SchemaRef,
    primary_keys: Vec<String>,
    /// Immutable decode layout behind an `Arc` swapped on a compatible ALTER
    /// (see [`adopt_current_layout`]). The pump clones the current `Arc` into a
    /// route at `TableMapEvent` time, so a snapshot handed downstream to
    /// [`MysqlChangeRows`] can never be mutated out from under a deferred decode.
    layout: Mutex<Arc<MemberLayout>>,
    sender: mpsc::Sender<std::result::Result<ChangeEnvelope, StreamError>>,
    metrics: Arc<MetricsCollector>,
    ready_lag: Duration,
    /// Durable per-dataset sidecar. The pump persists this member's committed
    /// position here on the checkpoint interval.
    position_store: Arc<dyn PositionStore>,
    /// Versioned checkpoint meta persisted alongside the position.
    checkpoint_schema_json: Option<String>,
}

/// One shared dump source: registry entry + pump state.
struct SharedSource {
    key: SourceKey,
    /// Connection-level params from the first subscriber (its `server_id` is
    /// the group's single id). Member-level knobs stay per-member.
    params: ReplicationParams,
    setup_lock: tokio::sync::Mutex<()>,
    members: Mutex<HashMap<MemberKey, Arc<MemberHandle>>>,
    ack: Arc<AckTable>,
    pump_started: AtomicBool,
    /// Asks the pump to drop its connection and reconnect from the current
    /// shared min — set when a member joins/rejoins so the dump repositions and
    /// promotion re-runs.
    restart_requested: AtomicBool,
    /// Set when the pump has exited (fatal error or all members detached).
    dead: AtomicBool,
    /// Tables whose member detached during this pump's lifetime; a rejoin is
    /// resumed from the held floor instead of a fresh snapshot.
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

    /// Detach a member: stop routing to it but hold its ack floor so the shared
    /// resume never advances past what it durably applied. `stalls_group`
    /// distinguishes a genuine unhealed stall (ERROR — its held floor now pins
    /// the shared resume and risks the purge cliff) from a self-healing
    /// supersede (WARN).
    fn detach_member(&self, key: &MemberKey, reason: &str, stalls_group: bool) {
        let removed = lock(&self.members).remove(key);
        let was_snapshotting = self.ack.detach(key);
        // A member that detaches mid-snapshot has an accelerator missing base
        // rows that binlog replay can never provide (its head placeholder was
        // never persisted — see `persist_all`), so it must NOT rejoin via the
        // held-floor path. Leaving it out of `detached` routes a rejoin back
        // through `resolve_start_position`, which re-snapshots.
        if !was_snapshotting {
            lock(&self.detached).insert(key.clone());
        }
        if let Some(member) = removed {
            member.metrics.mark_member_detached();
            if stalls_group {
                tracing::error!(
                    dataset = %member.dataset_name,
                    table = %format_member(key),
                    group = %self.key.group,
                    reason,
                    "shared mysql binlog member detached; its last applied position now pins the \
                     shared resume position for the whole group until the dataset rejoins or spiced \
                     restarts. If the source purges binlogs past it (binlog_expire_logs_seconds) the \
                     group must re-bootstrap (watch dataset_mysql_replication_member_attached and \
                     dataset_mysql_replication_lag_bytes)"
                );
            } else {
                tracing::warn!(
                    dataset = %member.dataset_name,
                    table = %format_member(key),
                    group = %self.key.group,
                    reason,
                    "shared mysql binlog member detached and is being replaced by a new \
                     subscription (rejoin in progress)"
                );
            }
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

/// Entry point: subscribe one dataset to its shared dump source. Mirrors the
/// per-dataset lazy contract — setup runs on first poll, errors surface through
/// the stream.
#[must_use]
pub fn subscribe(input: ReplicationStreamInput) -> ChangesStream {
    Box::pin(
        stream::once(async move { subscribe_inner(input).await }).flat_map(|result| match result {
            Ok(stream) => stream,
            Err(e) => stream::once(async move { Err(stream_error(&e)) }).boxed(),
        }),
    )
}

async fn subscribe_inner(input: ReplicationStreamInput) -> Result<ChangesStream> {
    let key = SourceKey::from_params(&input.params);
    let group = key.group.clone();

    // A source can die (pump exit) between fetching it and acquiring its setup
    // lock; retry against a fresh registry entry.
    for _attempt in 0..3 {
        let source = get_or_create_source(&key, &input.params);
        let guard = source.setup_lock.lock().await;
        if source.dead.load(Ordering::Acquire) {
            drop(guard);
            continue;
        }
        return attach_member(&source, input).await;
    }
    Err(Error::SharedSourceUnavailable { group })
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

/// Register one member on the source (caller holds the setup lock): validate
/// the source layout, decide the start position (resume vs snapshot), wire the
/// routing channel, and start the pump if this is the first member.
async fn attach_member(
    source: &Arc<SharedSource>,
    input: ReplicationStreamInput,
) -> Result<ChangesStream> {
    let ReplicationStreamInput {
        dataset_name,
        params,
        schema,
        primary_keys,
        database,
        table,
        position_store,
        schema_json,
        metrics,
    } = input;
    let member_key: MemberKey = (database.clone(), table.clone());
    let group = source.key.group.clone();

    // All members share ONE dump connection built from the first subscriber's
    // params, so their single `server_id` must agree (host/port/user/db are the
    // registry key already). A divergent explicit `mysql_replication_server_id`
    // would otherwise silently evict a slot-mate off the source.
    if params.server_id != source.params.server_id {
        return Err(Error::SharedConnectionParamsMismatch {
            dataset: dataset_name,
            group,
            param: "mysql_replication_server_id",
        });
    }
    // The whole group rides ONE dump connection built from the first
    // subscriber's opts, and the `SourceKey` intentionally excludes the
    // password — so reject a member whose password differs rather than
    // silently authenticating it with a slot-mate's credential.
    if params.opts.pass() != source.params.opts.pass() {
        return Err(Error::SharedConnectionParamsMismatch {
            dataset: dataset_name,
            group,
            param: "mysql_pass",
        });
    }

    if let Some(existing) = source.member(&member_key) {
        if existing.sender.is_closed() {
            source.detach_member(&member_key, "superseded by a new subscription", false);
        } else {
            return Err(Error::SharedTableAlreadySubscribed {
                database,
                table,
                group,
            });
        }
    }

    // Validate the source + fetch the positional layout.
    let mut conn = super::setup::connect(&params).await?;
    super::setup::validate_server(&mut conn).await?;
    let layout = super::setup::fetch_table_layout(&mut conn, &database, &table).await?;
    let column_map = layout.column_map(&schema, &database, &table)?;
    for pk in &primary_keys {
        if !layout.columns.iter().any(|c| c.name == *pk) {
            if let Err(e) = conn.disconnect().await {
                tracing::debug!(dataset = %dataset_name, error = %e, "setup disconnect");
            }
            return super::SchemaMismatchSnafu {
                message: format!(
                    "declared primary_key `{pk}` not found on source table {database}.{table}"
                ),
            }
            .fail();
        }
    }
    let pk_source_indexes = compute_pk_source_indexes(&schema, &primary_keys, &column_map);
    let layout_fingerprint = layout.fingerprint();
    let checkpoint_schema_json = encode_checkpoint_schema_json(schema_json.as_deref(), &layout);

    // Decide the start position from the member's OWN persisted sidecar — the
    // MySQL source of truth (no server-side cursor). Crucially this runs on
    // EVERY (re)subscribe, including a rejoin: `resolve_start_position` checks
    // the persisted checkpoint's layout fingerprint against the freshly-fetched
    // current source layout, so a schema change that happened while the member
    // was detached triggers `invalid_checkpoint_behavior` (rebootstrap/error)
    // instead of replaying pre-change row images with the post-change layout —
    // which would silently scramble columns. There is no blind held-floor
    // fast-path (the per-dataset `Checkpointer`'s replay-boundary machinery that
    // would make one safe is intentionally out of v1 scope).
    let rejoining = lock(&source.detached).remove(&member_key);
    let (floor, snapshotting): (BinlogPosition, bool) = resolve_start_position(
        &mut conn,
        &params,
        &position_store,
        schema_json.as_deref(),
        checkpoint_schema_json.as_deref(),
        &layout_fingerprint,
        &dataset_name,
    )
    .await?;
    if let Err(e) = conn.disconnect().await {
        tracing::debug!(dataset = %dataset_name, error = %e, "setup disconnect");
    }

    let (sender, receiver) = mpsc::channel(DEFAULT_MEMBER_CHANNEL_CAPACITY);
    source.ack.register(&member_key, floor, snapshotting);
    lock(&source.members).insert(
        member_key.clone(),
        Arc::new(MemberHandle {
            dataset_name: dataset_name.clone(),
            schema: Arc::clone(&schema),
            primary_keys: primary_keys.clone(),
            layout: Mutex::new(Arc::new(MemberLayout {
                layout: layout.clone(),
                column_map: column_map.clone(),
                pk_source_indexes,
            })),
            sender,
            metrics: Arc::clone(&metrics),
            ready_lag: params.ready_lag,
            position_store: Arc::clone(&position_store),
            checkpoint_schema_json: checkpoint_schema_json.clone(),
        }),
    );
    metrics.mark_member_attached();

    tracing::info!(
        dataset = %dataset_name,
        table = %format_member(&member_key),
        group = %source.key.group,
        snapshot = snapshotting,
        rejoining,
        members = source.live_member_count(),
        "dataset joined shared mysql binlog group"
    );

    if !source.pump_started.swap(true, Ordering::AcqRel) {
        let pump_source = Arc::clone(source);
        tokio::spawn(run_pump(pump_source));
    } else if !snapshotting {
        // A resuming/rejoining member needs the pump to reconnect so it
        // repositions to the (possibly lower) new min and re-runs promotion. A
        // snapshotting member stays held until its completion hook requests the
        // reconnect, so a crash-looping snapshot can't force a reconnect storm.
        source.restart_requested.store(true, Ordering::Release);
    }

    // Head of the member's stream: initial snapshot (whose completion hook
    // promotes it and reconnects), or an immediate empty head on resume.
    let head: ChangesStream = if snapshotting {
        let dataset_for_boundary = dataset_name.clone();
        let snapshot = super::bootstrap::snapshot_stream(super::bootstrap::SnapshotInput {
            params: params.clone(),
            layout,
            schema: Arc::clone(&schema),
            primary_keys,
            column_map,
            database,
            table,
            dataset_name,
            metrics: Arc::clone(&metrics),
        });
        // Snapshot completion is signalled by a real boundary committer, NOT a
        // stream-drain hook: the committer's `commit()` runs on the apply loop
        // only after every prior (snapshot) envelope is durably applied, whereas
        // a drain hook fires when the reader pulls it — up to a prefetch-channel
        // ahead of durable apply. Promoting + persisting this member's head
        // before its snapshot is durably applied would lose base rows on a
        // crash. On a snapshot error the stream ends before this envelope, so
        // the member stays SNAPSHOTTING and re-snapshots on rejoin (see
        // `detach_member`). `persist_all` also skips SNAPSHOTTING members until
        // this fires.
        let (_, boundary_batch, _) = crate::cdc::build_heartbeat_envelope(&schema, None, false)
            .map_err(|e| Error::SchemaMismatch {
                message: e.to_string(),
            })?
            .into_parts()
            .map_err(|e| Error::SchemaMismatch {
                message: e.to_string(),
            })?;
        let boundary = ChangeEnvelope::from_parts(
            Box::new(SnapshotBoundaryCommitter {
                source: Arc::clone(source),
                key: member_key.clone(),
                dataset: dataset_for_boundary,
            }),
            boundary_batch,
            false,
        );
        Box::pin(snapshot.chain(stream::once(async move { Ok(boundary) })))
    } else {
        metrics.mark_bootstrap_complete();
        Box::pin(stream::empty::<
            std::result::Result<ChangeEnvelope, StreamError>,
        >())
    };

    Ok(Box::pin(head.chain(ReceiverStream::new(receiver))))
}

/// Resolve a fresh (non-rejoin) member's start position, applying
/// `invalid_checkpoint_behavior` per member. Returns `(floor, snapshotting)`.
async fn resolve_start_position(
    conn: &mut Conn,
    params: &ReplicationParams,
    position_store: &Arc<dyn PositionStore>,
    schema_json: Option<&str>,
    checkpoint_schema_json: Option<&str>,
    layout_fingerprint: &str,
    dataset_name: &str,
) -> Result<(BinlogPosition, bool)> {
    let persisted = position_store
        .load()
        .await
        .map_err(|e| Error::PositionStoreAccess {
            message: e.to_string(),
        })?;

    let resume: Option<BinlogPosition> = match persisted {
        Some(_) if params.snapshot_mode == InitialSnapshotMode::Enabled => None,
        Some(persisted) => {
            if check_resume_compatibility(
                persisted.schema_json.as_deref(),
                schema_json,
                layout_fingerprint,
            )
            .is_err()
            {
                apply_invalid_checkpoint(
                    params,
                    position_store,
                    dataset_name,
                    "layout/schema drift",
                )
                .await?;
                None
            } else if super::setup::binlog_file_exists(conn, &persisted.position.file).await? {
                Some(persisted.position)
            } else {
                apply_invalid_checkpoint(params, position_store, dataset_name, "binlog purged")
                    .await?;
                None
            }
        }
        None => None,
    };

    if let Some(position) = resume {
        tracing::info!(dataset = %dataset_name, position = %position, "shared mysql binlog: resuming from persisted position");
        return Ok((position, false));
    }

    // No usable position: capture the head first (so the snapshot overlap
    // replays idempotently) and either snapshot from it or (snapshot disabled)
    // stream from it after persisting it up front.
    let head = super::setup::fetch_head_position(conn).await?;
    if params.snapshot_mode == InitialSnapshotMode::Disabled {
        let initial = PersistedPosition {
            position: head.clone(),
            schema_json: checkpoint_schema_json.map(ToString::to_string),
        };
        if let Err(e) = position_store.save(&initial).await {
            tracing::warn!(dataset = %dataset_name, error = %e, "failed to persist initial binlog head");
        }
        Ok((head, false))
    } else {
        Ok((head, true))
    }
}

/// Apply `invalid_checkpoint_behavior` for one member: `Error` fails the
/// member's stream; `Restart` clears its saved position so it re-snapshots.
async fn apply_invalid_checkpoint(
    params: &ReplicationParams,
    position_store: &Arc<dyn PositionStore>,
    dataset_name: &str,
    reason: &str,
) -> Result<()> {
    match params.invalid_position_behavior {
        InvalidCheckpointBehavior::Error => super::StalePositionSnafu {
            message: format!(
                "cannot resume mysql binlog for {dataset_name} ({reason}). Replaying against the \
                 current source layout would mis-map columns. Set \
                 `mysql_replication_invalid_checkpoint_behavior: restart` to drop the saved \
                 position and re-snapshot the table."
            ),
        }
        .fail(),
        InvalidCheckpointBehavior::Restart => {
            tracing::warn!(dataset = %dataset_name, reason, "persisted mysql binlog checkpoint unusable; rebootstrapping");
            if let Err(e) = position_store.clear().await {
                tracing::warn!(dataset = %dataset_name, error = %e, "failed to clear unusable binlog position");
            }
            Ok(())
        }
    }
}

/// Mark the source dead and drop it from the registry (only if the registry
/// still points at this instance).
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
/// detached. Takes the setup lock so a subscriber can never register on a
/// source concurrently finalizing its death.
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

/// Send a fatal error to every member and terminate the source.
async fn fatal_broadcast(source: &Arc<SharedSource>, message: String) {
    tracing::error!(group = %source.key.group, "shared mysql binlog stream failed: {message}");
    for (_, member) in source.live_members() {
        let _ = member
            .sender
            .send(Err(StreamError::External(message.clone())))
            .await;
    }
}

/// Send a member-scoped fatal error and detach the member (holding its floor).
async fn member_fatal(source: &Arc<SharedSource>, key: &MemberKey, message: String) {
    if let Some(member) = source.member(key) {
        let _ = member
            .sender
            .send(Err(StreamError::External(message)))
            .await;
    }
    source.detach_member(key, "fatal member error", true);
}

/// One resolved route for a `table_id` on the current connection. `tme` and
/// `layout` are the immutable snapshots taken at `TableMapEvent` install, so a
/// deferred [`MysqlChangeRows`] decodes against exactly the layout that was
/// valid when its rows were written.
struct Route {
    key: MemberKey,
    member: Arc<MemberHandle>,
    slot: Arc<AckSlot>,
    tme: Arc<TableMapEvent<'static>>,
    layout: Arc<MemberLayout>,
}

/// The shared pump: one binlog dump driving every member.
#[expect(
    clippy::too_many_lines,
    reason = "single state machine over the multiplexed binlog event loop; mirrors the \
              per-dataset binlog_change_stream and postgres run_pump"
)]
async fn run_pump(source: Arc<SharedSource>) {
    let shutdown_epoch = crate::cdc::shutdown_epoch();
    let params = source.params.clone();
    let group = source.key.group.clone();
    let mut backoff = super::resilience::StreamBackoff::default_for_stream();
    let mut reconnect_attempts: u32 = 0;
    let idle_tick = crate::cdc::heartbeat_interval(params.ready_lag)
        .min(params.checkpoint_interval)
        .min(IDLE_TICK_CAP);
    let mut side_conn: Option<Conn> = None;
    let mut last_persist_at = Instant::now();
    // Last position persisted per member, to skip no-op sidecar writes.
    let mut last_persisted: HashMap<MemberKey, BinlogPosition> = HashMap::new();

    'reconnect: loop {
        if crate::cdc::shutdown_epoch() != shutdown_epoch {
            persist_all(&source, &mut last_persisted).await;
            tracing::info!(group = %group, "runtime shutdown; releasing shared mysql binlog connection");
            finish_pump(&source);
            return;
        }
        source.reap_closed_members();
        if source.live_member_count() == 0 && try_finish_if_empty(&source).await {
            tracing::info!(group = %group, "all members detached; shutting down shared mysql binlog stream");
            return;
        }
        source.restart_requested.store(false, Ordering::Release);

        let Some(resume) = source.ack.flush_position() else {
            // No members yet (raced a detach); loop to re-check / finish.
            continue 'reconnect;
        };

        let mut stream = match open_binlog_stream(&params, &resume, &group).await {
            Ok(stream) => {
                backoff.reset();
                if reconnect_attempts > 0 {
                    tracing::info!(group = %group, attempts = reconnect_attempts, position = %resume, "shared mysql binlog connection resumed");
                    reconnect_attempts = 0;
                }
                // Connection starts at the shared min (<= every held member's
                // floor), so every snapshot-complete member's gap is covered:
                // promote them to routable + creditable.
                source.ack.promote_ready_members();
                stream
            }
            Err(e) if super::resilience::is_purged_position_error(&e) => {
                // The shared min was purged from the source. Surface it to every
                // member; on reload/restart each member re-evaluates its own
                // persisted position via `invalid_checkpoint_behavior`.
                fatal_broadcast(&source, purged_position_error(&resume, &group).to_string()).await;
                break 'reconnect;
            }
            Err(e) if super::resilience::is_transient_mysql(&e) => {
                for (_, m) in source.live_members() {
                    m.metrics.inc_reconnect();
                }
                reconnect_attempts = reconnect_attempts.saturating_add(1);
                log_transient_reconnect(
                    reconnect_attempts,
                    &group,
                    &e.to_string(),
                    backoff.next_delay().as_millis(),
                );
                backoff.wait().await;
                continue 'reconnect;
            }
            Err(e) => {
                fatal_broadcast(&source, format!("fatal mysql binlog connect failed: {e}")).await;
                break 'reconnect;
            }
        };

        // Per-connection routing + buffering state (rebuilt each connection —
        // the dump re-sends rotate + TableMap events). Keyed by the server's
        // `table_id`: a trusted server-assigned integer with no HashDoS surface,
        // so `FxHashMap` (a fast u64 hash) is preferred over std's SipHash. `txn`
        // buffers OWNED raw row payloads per table; the tuple decode + Arrow
        // build run off the pump in `MysqlChangeRows::build`.
        let mut routes: FxHashMap<u64, Route> = FxHashMap::default();
        let mut txn: FxHashMap<u64, Vec<RowsEventData<'static>>> = FxHashMap::default();
        let mut txn_open = false;
        let mut current_file = resume.file.clone();

        'recv: loop {
            if crate::cdc::shutdown_epoch() != shutdown_epoch {
                if let Err(e) = stream.close().await {
                    tracing::debug!(group = %group, error = %e, "binlog close during shutdown");
                }
                persist_all(&source, &mut last_persisted).await;
                tracing::info!(group = %group, "runtime shutdown; released shared mysql binlog connection");
                finish_pump(&source);
                return;
            }
            if source.restart_requested.swap(false, Ordering::AcqRel) {
                tracing::debug!(group = %group, "reconnecting shared mysql binlog stream to pick up membership change");
                break 'recv;
            }
            source.reap_closed_members();
            if source.live_member_count() == 0 && try_finish_if_empty(&source).await {
                tracing::info!(group = %group, "all members detached; shutting down shared mysql binlog stream");
                return;
            }

            let next_event = match tokio::time::timeout(idle_tick, stream.next()).await {
                Ok(item) => item,
                Err(_idle) => {
                    if last_persist_at.elapsed() >= params.checkpoint_interval {
                        persist_all(&source, &mut last_persisted).await;
                        last_persist_at = Instant::now();
                    }
                    poll_head_and_heartbeat(&source, &mut side_conn, &params).await;
                    continue 'recv;
                }
            };
            let Some(event) = next_event else {
                for (_, m) in source.live_members() {
                    m.metrics.inc_recv_error();
                    m.metrics.inc_reconnect();
                }
                reconnect_attempts = reconnect_attempts.saturating_add(1);
                log_transient_reconnect(
                    reconnect_attempts,
                    &group,
                    "server closed the binlog stream",
                    backoff.next_delay().as_millis(),
                );
                break 'recv;
            };
            let event = match event {
                Ok(event) => event,
                Err(e) if super::resilience::is_purged_position_error(&e) => {
                    let resume = source
                        .ack
                        .flush_position()
                        .unwrap_or_else(|| current_file_pos(&current_file));
                    fatal_broadcast(&source, purged_position_error(&resume, &group).to_string())
                        .await;
                    break 'reconnect;
                }
                Err(e) if super::resilience::is_transient_mysql(&e) => {
                    for (_, m) in source.live_members() {
                        m.metrics.inc_recv_error();
                        m.metrics.inc_reconnect();
                    }
                    reconnect_attempts = reconnect_attempts.saturating_add(1);
                    log_transient_reconnect(
                        reconnect_attempts,
                        &group,
                        &e.to_string(),
                        backoff.next_delay().as_millis(),
                    );
                    break 'recv;
                }
                Err(e) => {
                    fatal_broadcast(&source, format!("mysql binlog recv failed: {e}")).await;
                    break 'reconnect;
                }
            };

            let header = event.header();
            let event_end_pos = u64::from(header.log_pos());
            let event_timestamp = header.timestamp();
            let data = match event.read_data() {
                Ok(data) => data,
                Err(e) => {
                    fatal_broadcast(&source, format!("mysql binlog event decode failed: {e}"))
                        .await;
                    break 'reconnect;
                }
            };

            match data {
                Some(EventData::RotateEvent(rotate)) => {
                    if !rotate.is_fake() {
                        current_file = rotate.name().into_owned();
                    }
                }
                Some(EventData::TableMapEvent(tme)) => {
                    let table_id = tme.table_id();
                    let mkey: MemberKey = (
                        tme.database_name().to_string(),
                        tme.table_name().to_string(),
                    );
                    // The (database, table) match is validated HERE, once, by the
                    // member lookup keyed on the decoded name — the per-rows-event
                    // path is then a bare `routes.get(table_id)` with no re-derived
                    // table-map filter or string compare.
                    let Some(member) = source.member(&mkey) else {
                        routes.remove(&table_id);
                        continue 'recv;
                    };
                    let Some(slot) = source.ack.slot(&mkey) else {
                        routes.remove(&table_id);
                        continue 'recv;
                    };
                    if !slot.has(STREAMING) {
                        // Held (snapshotting or joined after this connection
                        // started) — don't route; the next reconnect promotes it.
                        routes.remove(&table_id);
                        continue 'recv;
                    }
                    // Compatible mid-stream ALTER: adopt the current source layout
                    // by swapping the member's layout `Arc`. Incompatible →
                    // member-fatal (this member only; the group keeps running).
                    let needs_adopt = {
                        let g = lock(&member.layout);
                        tme.columns_count() != g.layout.columns.len() as u64
                    };
                    if needs_adopt {
                        let old_layout = { lock(&member.layout).layout.clone() };
                        match adopt_current_layout(
                            &params,
                            &mkey.0,
                            &mkey.1,
                            &member.schema,
                            &old_layout,
                            &member.primary_keys,
                            &member.dataset_name,
                        )
                        .await
                        {
                            Ok(AdoptedLayout {
                                layout,
                                column_map,
                                pk_source_indexes,
                            }) if layout.columns.len() as u64 == tme.columns_count() => {
                                *lock(&member.layout) = Arc::new(MemberLayout {
                                    layout,
                                    column_map,
                                    pk_source_indexes,
                                });
                            }
                            outcome => {
                                member.metrics.inc_schema_mismatch_error();
                                routes.remove(&table_id);
                                let detail = match outcome {
                                    Ok(_) => "the current source layout column count still disagrees with the event".to_string(),
                                    Err(e) => e.to_string(),
                                };
                                member_fatal(&source, &mkey, format!(
                                    "source table {}.{} changed shape and the new layout cannot be adopted: {detail}. Re-bootstrap by setting `mysql_replication_invalid_checkpoint_behavior: restart`.",
                                    mkey.0, mkey.1
                                )).await;
                                continue 'recv;
                            }
                        }
                    }
                    // Snapshot the columns + the (possibly just-adopted) layout so
                    // a deferred decode uses exactly the layout valid now, even if
                    // a later ALTER swaps the member's layout `Arc`.
                    let layout = {
                        let g = lock(&member.layout);
                        Arc::clone(&g)
                    };
                    let tme = Arc::new(tme.into_owned());
                    routes.insert(
                        table_id,
                        Route {
                            key: mkey,
                            member,
                            slot,
                            tme,
                            layout,
                        },
                    );
                }
                Some(EventData::RowsEvent(rows_data)) => {
                    let table_id = rows_data.table_id();
                    let Some(route) = routes.get(&table_id) else {
                        continue 'recv;
                    };
                    if !route.slot.has(STREAMING) {
                        continue 'recv;
                    }
                    // Hot path: buffer the OWNED raw payload only — no tuple decode,
                    // no Arrow build, no layout lock, no per-row metrics here. The
                    // decode runs later on the per-dataset consumer in
                    // `MysqlChangeRows::build`; a malformed row then faults only
                    // that dataset's stream, never the shared pump.
                    txn.entry(table_id)
                        .or_default()
                        .push(rows_data.into_owned());
                }
                Some(EventData::XidEvent(_)) => {
                    txn_open = false;
                    let commit_pos = BinlogPosition::new(current_file.clone(), event_end_pos);
                    deliver_commit(
                        &source,
                        &routes,
                        std::mem::take(&mut txn),
                        &commit_pos,
                        event_timestamp,
                        shutdown_epoch,
                    )
                    .await;
                }
                Some(EventData::QueryEvent(query)) => {
                    let statement = query.query();
                    let default_db = query.schema();
                    match classify_query(&statement) {
                        QueryKind::Begin => {
                            txn_open = true;
                            txn.clear();
                        }
                        QueryKind::Commit => {
                            txn_open = false;
                            let commit_pos =
                                BinlogPosition::new(current_file.clone(), event_end_pos);
                            deliver_commit(
                                &source,
                                &routes,
                                std::mem::take(&mut txn),
                                &commit_pos,
                                event_timestamp,
                                shutdown_epoch,
                            )
                            .await;
                        }
                        QueryKind::Xa => {
                            tracing::warn!(group = %group, statement = %statement, "XA transaction observed on the shared binlog; XA transactions are not supported and their changes are ignored");
                        }
                        QueryKind::Statement => {
                            handle_statement(
                                &source,
                                &statement,
                                &default_db,
                                &current_file,
                                event_end_pos,
                                event_timestamp,
                                shutdown_epoch,
                            )
                            .await;
                            // Auto-commit DDL (TRUNCATE/ALTER/…) closes the GTID
                            // group it arrived in without an Xid/COMMIT. Under ROW
                            // format row changes never arrive as statements, so the
                            // buffer is empty — drop it and reopen the idle
                            // safe-advance, else `txn_open` stays stuck true and the
                            // shared resume never advances past foreign traffic again.
                            txn_open = false;
                            txn.clear();
                        }
                    }
                }
                Some(EventData::GtidEvent(_) | EventData::AnonymousGtidEvent(_)) => {
                    txn_open = true;
                }
                Some(_) | None => {}
            }

            // Idle safe-advance: with no open transaction, everything up to this
            // event's end is either applied or irrelevant to every member. Credit
            // idle streaming members so a group with quiet members still advances
            // its resume past foreign-table traffic.
            if !txn_open && event_end_pos >= MIN_VALID_EVENT_POS {
                let pos = BinlogPosition::new(current_file.clone(), event_end_pos);
                source.ack.credit_idle(&pos);
            }

            if last_persist_at.elapsed() >= params.checkpoint_interval {
                persist_all(&source, &mut last_persisted).await;
                last_persist_at = Instant::now();
            }
        } // 'recv

        persist_all(&source, &mut last_persisted).await;
        backoff.wait().await;
    } // 'reconnect

    // Fatal exit: error any member still attached and finalize.
    let _guard = source.setup_lock.lock().await;
    for (_, member) in source.live_members() {
        let _ = member
            .sender
            .send(Err(StreamError::External(
                "shared mysql binlog stream terminated".to_string(),
            )))
            .await;
    }
    finish_pump(&source);
}

fn current_file_pos(file: &str) -> BinlogPosition {
    BinlogPosition::new(file.to_string(), 0)
}

/// Deliver a committed transaction's buffered rows to their members, then
/// credit idle streaming members up to the commit position. The pump does the
/// O(1)-per-table work only (route lookup, watermark, transaction count, commit
/// bookkeeping); the tuple decode + Arrow build are deferred into the
/// per-dataset consumer via [`MysqlChangeRows`].
async fn deliver_commit(
    source: &Arc<SharedSource>,
    routes: &FxHashMap<u64, Route>,
    txn: FxHashMap<u64, Vec<RowsEventData<'static>>>,
    commit_pos: &BinlogPosition,
    event_timestamp: u32,
    shutdown_epoch: u64,
) {
    let commit_ts = commit_ts_ms(event_timestamp);
    for (table_id, events) in txn {
        if events.is_empty() {
            continue;
        }
        let Some(Route {
            key,
            member,
            slot,
            tme,
            layout,
        }) = routes.get(&table_id)
        else {
            continue;
        };
        if !slot.has(STREAMING) || slot.already_committed(commit_pos) {
            continue;
        }
        // O(1), no decode: freshness watermark + transaction count. The per-row
        // op counters are recorded off-pump in `MysqlChangeRows::build`.
        record_watermark(&member.metrics, event_timestamp);
        member.metrics.inc_transaction();
        let is_ready = crate::cdc::source_commit_within_ready_lag(commit_ts, member.ready_lag);
        // Hand the OWNED payload + immutable layout/columns snapshots downstream;
        // the decode + build runs on the consumer, and a decode failure surfaces
        // as a `StreamError` on this one dataset's stream.
        let rows = MysqlChangeRows::new(
            Arc::clone(&member.schema),
            member.primary_keys.clone(),
            Arc::clone(layout),
            Arc::clone(tme),
            events,
            commit_ts,
            Arc::clone(&member.metrics),
        );
        let envelope = ChangeEnvelope::new_from_rows(
            Box::new(SharedPositionCommitter {
                slot: Arc::clone(slot),
                flush_to: commit_pos.clone(),
                dataset: member.dataset_name.clone(),
                source_commit_ts_ms: commit_ts,
            }),
            Box::new(rows),
            is_ready,
        );
        slot.deliver(commit_pos);
        match deliver_to_member(&member.sender, Ok(envelope), shutdown_epoch).await {
            DeliverOutcome::Sent => {}
            DeliverOutcome::ReceiverGone => {
                source.detach_member(key, "changes stream receiver dropped", true);
            }
            DeliverOutcome::ShutdownAbandon => return,
        }
    }
    source.ack.credit_idle(commit_pos);
}

/// Route a statement affecting a subscribed table: TRUNCATE is applied as a
/// change; a schema-change DDL is member-fatal (adoption happens on the next
/// `TableMap` for compatible changes).
async fn handle_statement(
    source: &Arc<SharedSource>,
    statement: &str,
    default_db: &str,
    current_file: &str,
    event_end_pos: u64,
    event_timestamp: u32,
    shutdown_epoch: u64,
) {
    // A statement can target any subscribed table; scan the live members.
    for (mkey, member) in source.live_members() {
        let Some(kind) = classify_statement(statement, default_db, &mkey.0, &mkey.1) else {
            continue;
        };
        let Some(slot) = source.ack.slot(&mkey) else {
            continue;
        };
        if !slot.has(STREAMING) {
            continue;
        }
        match kind {
            StatementKind::Truncate => {
                let commit_pos = BinlogPosition::new(current_file.to_string(), event_end_pos);
                if slot.already_committed(&commit_pos) {
                    continue;
                }
                member.metrics.inc_truncate();
                member.metrics.inc_transaction();
                record_watermark(&member.metrics, event_timestamp);
                let commit_ts = commit_ts_ms(event_timestamp);
                let batch = {
                    let g = lock(&member.layout);
                    build_change_batch(
                        &member.schema,
                        &member.primary_keys,
                        &g.column_map,
                        &[truncate_change()],
                    )
                };
                let batch = match batch {
                    Ok(b) => b.with_source_commit_ts_ms(commit_ts),
                    Err(e) => {
                        member_fatal(
                            source,
                            &mkey,
                            format!("truncate batch build failed for {}.{}: {e}", mkey.0, mkey.1),
                        )
                        .await;
                        continue;
                    }
                };
                let is_ready =
                    crate::cdc::source_commit_within_ready_lag(commit_ts, member.ready_lag);
                let envelope = ChangeEnvelope::new(
                    Box::new(SharedPositionCommitter {
                        slot: Arc::clone(&slot),
                        flush_to: commit_pos.clone(),
                        dataset: member.dataset_name.clone(),
                        source_commit_ts_ms: commit_ts,
                    }),
                    batch,
                    is_ready,
                );
                slot.deliver(&commit_pos);
                match deliver_to_member(&member.sender, Ok(envelope), shutdown_epoch).await {
                    DeliverOutcome::Sent => {}
                    DeliverOutcome::ReceiverGone => {
                        source.detach_member(&mkey, "changes stream receiver dropped", true);
                    }
                    DeliverOutcome::ShutdownAbandon => return,
                }
            }
            StatementKind::SchemaChange(verb) => {
                // ALTER is adopted on the following TableMap (column-count change);
                // DROP/RENAME (and an ALTER that drops a dataset column) is fatal
                // for this member. Only the affected member is torn down.
                member.metrics.inc_schema_mismatch_error();
                member_fatal(source, &mkey, format!(
                    "{verb} detected on source table {}.{} (statement: {statement}). Fix the source (or dataset) and re-bootstrap by setting `mysql_replication_invalid_checkpoint_behavior: restart`.",
                    mkey.0, mkey.1
                )).await;
            }
        }
    }
}

enum DeliverOutcome {
    Sent,
    ReceiverGone,
    ShutdownAbandon,
}

/// Must-deliver one envelope into a member's bounded channel, warning on a
/// prolonged stall and abandoning on shutdown. One slow member must not wedge
/// the pump (and thus the group) or block shutdown indefinitely.
async fn deliver_to_member(
    sender: &mpsc::Sender<std::result::Result<ChangeEnvelope, StreamError>>,
    envelope: std::result::Result<ChangeEnvelope, StreamError>,
    shutdown_epoch: u64,
) -> DeliverOutcome {
    let mut pending = envelope;
    loop {
        match sender.send_timeout(pending, MEMBER_SEND_STALL_WARN).await {
            Ok(()) => return DeliverOutcome::Sent,
            Err(mpsc::error::SendTimeoutError::Closed(_)) => return DeliverOutcome::ReceiverGone,
            Err(mpsc::error::SendTimeoutError::Timeout(returned)) => {
                if crate::cdc::shutdown_epoch() != shutdown_epoch {
                    return DeliverOutcome::ShutdownAbandon;
                }
                tracing::warn!(stalled_for = ?MEMBER_SEND_STALL_WARN, "shared mysql binlog member sink is not draining; the pump is waiting to deliver committed changes");
                pending = returned;
            }
        }
    }
}

/// Persist each member's own committed position to its own sidecar (skipping
/// no-op writes). The shared resume is the min across these on restart.
async fn persist_all(source: &Arc<SharedSource>, last: &mut HashMap<MemberKey, BinlogPosition>) {
    for (key, member) in source.live_members() {
        let Some(slot) = source.ack.slot(&key) else {
            continue;
        };
        // A snapshotting member's `committed` is a head placeholder whose base
        // rows are not yet durably applied; persisting it and crashing would
        // resume past an un-applied snapshot (missing base rows). Skip until the
        // `SnapshotBoundaryCommitter` clears SNAPSHOTTING post-apply.
        if slot.has(SNAPSHOTTING) {
            continue;
        }
        let committed = slot.committed();
        if last.get(&key) == Some(&committed) {
            continue;
        }
        let persisted = PersistedPosition {
            position: committed.clone(),
            schema_json: member.checkpoint_schema_json.clone(),
        };
        match member.position_store.save(&persisted).await {
            Ok(()) => {
                member.metrics.inc_checkpoint_persist();
                member
                    .metrics
                    .set_committed_position(committed.file_ordinal().unwrap_or(0), committed.pos);
                last.insert(key, committed);
            }
            Err(e) => {
                member.metrics.inc_checkpoint_persist_error();
                tracing::warn!(dataset = %member.dataset_name, error = %e, "failed to persist shared mysql binlog position");
            }
        }
    }
}

/// Poll the source head over a lazily-maintained side connection, publish the
/// head/lag metrics to every member, and fan a readiness heartbeat to every
/// caught-up streaming member. Best-effort — never disturbs replication.
async fn poll_head_and_heartbeat(
    source: &Arc<SharedSource>,
    side_conn: &mut Option<Conn>,
    params: &ReplicationParams,
) {
    let Some(resume) = source.ack.flush_position() else {
        return;
    };
    let conn = match side_conn {
        Some(conn) => conn,
        None => match Conn::new(params.opts.clone()).await {
            Ok(conn) => side_conn.insert(conn),
            Err(e) => {
                tracing::debug!(group = %source.key.group, error = %e, "shared head-poll connect failed");
                return;
            }
        },
    };
    let head = match super::setup::fetch_head_position(conn).await {
        Ok(head) => head,
        Err(e) => {
            tracing::debug!(group = %source.key.group, error = %e, "shared head poll failed");
            *side_conn = None;
            return;
        }
    };
    for (_, member) in source.live_members() {
        let lag = (head.file == resume.file).then(|| head.pos.saturating_sub(resume.pos));
        member
            .metrics
            .set_source_head(head.file_ordinal().unwrap_or(0), head.pos, lag);
    }
    // Only once the group has caught up to the head may a heartbeat carry a
    // fresh "current" clock.
    if resume < head {
        return;
    }
    let source_now_ms = match super::setup::fetch_source_now_ms(conn).await {
        Ok(ms) => ms,
        Err(e) => {
            tracing::debug!(group = %source.key.group, error = %e, "shared source-clock query failed");
            *side_conn = None;
            return;
        }
    };
    for (key, member) in source.live_members() {
        if !source.ack.is_streaming(&key) {
            continue;
        }
        match readiness_heartbeat(
            &member.schema,
            source_now_ms,
            member.ready_lag,
            &member.dataset_name,
        ) {
            Ok(hb) => {
                let _ = member.sender.try_send(Ok(hb));
            }
            Err(e) => {
                tracing::warn!(dataset = %member.dataset_name, error = %e, "failed to build shared mysql heartbeat; skipping");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(db: &str, t: &str) -> MemberKey {
        (db.to_string(), t.to_string())
    }
    fn pos(file: &str, p: u64) -> BinlogPosition {
        BinlogPosition::new(file, p)
    }

    #[test]
    fn flush_position_is_min_across_members() {
        let ack = AckTable::default();
        ack.register(&key("db", "a"), pos("binlog.000001", 500), false);
        ack.register(&key("db", "b"), pos("binlog.000001", 500), false);
        ack.promote_ready_members();
        // Advance a ahead; the min is pinned by b.
        ack.slot(&key("db", "a"))
            .expect("slot")
            .commit(&pos("binlog.000001", 900));
        assert_eq!(ack.flush_position(), Some(pos("binlog.000001", 500)));
        ack.slot(&key("db", "b"))
            .expect("slot")
            .commit(&pos("binlog.000001", 900));
        assert_eq!(ack.flush_position(), Some(pos("binlog.000001", 900)));
    }

    #[test]
    fn held_member_is_never_credited_until_promoted() {
        let ack = AckTable::default();
        // Snapshotting member: held (LIVE | SNAPSHOTTING), not STREAMING.
        ack.register(&key("db", "a"), pos("binlog.000001", 100), true);
        ack.credit_idle(&pos("binlog.000001", 900));
        assert_eq!(
            ack.committed(&key("db", "a")),
            Some(pos("binlog.000001", 100)),
            "held member must not be credited"
        );
        // Complete snapshot + promote, then credit.
        ack.snapshot_finished(&key("db", "a"));
        ack.promote_ready_members();
        ack.credit_idle(&pos("binlog.000001", 900));
        assert_eq!(
            ack.committed(&key("db", "a")),
            Some(pos("binlog.000001", 900))
        );
    }

    #[test]
    fn detached_member_holds_the_floor() {
        let ack = AckTable::default();
        ack.register(&key("db", "a"), pos("binlog.000001", 500), false);
        ack.register(&key("db", "b"), pos("binlog.000001", 500), false);
        ack.promote_ready_members();
        // b detaches at 500; a advances.
        assert!(!ack.detach(&key("db", "b")));
        ack.slot(&key("db", "a"))
            .expect("slot")
            .commit(&pos("binlog.000010", 8000));
        // The detached member's frozen floor pins the min by design.
        assert_eq!(ack.flush_position(), Some(pos("binlog.000001", 500)));
    }

    #[test]
    fn detach_reports_snapshotting_state() {
        // `detach_member` relies on this to keep a mid-snapshot member out of
        // the `detached` set so a rejoin re-snapshots rather than resuming from
        // an un-applied head placeholder.
        let ack = AckTable::default();
        ack.register(&key("db", "a"), pos("binlog.000001", 100), true);
        assert!(
            ack.detach(&key("db", "a")),
            "detach must report snapshotting"
        );
        ack.register(&key("db", "b"), pos("binlog.000001", 100), false);
        ack.promote_ready_members();
        assert!(
            !ack.detach(&key("db", "b")),
            "a streaming member is not snapshotting"
        );
    }

    #[test]
    fn rejoin_resets_slot_to_resolved_floor() {
        let ack = AckTable::default();
        ack.register(&key("db", "a"), pos("binlog.000001", 500), false);
        ack.promote_ready_members();
        let slot = ack.slot(&key("db", "a")).expect("slot");
        slot.commit(&pos("binlog.000001", 700));
        slot.deliver(&pos("binlog.000001", 900));
        ack.detach(&key("db", "a"));
        // Rejoin re-resolves the start position from the member's sidecar
        // (layout-checked) and RESETS the slot to it — here a resume at 650,
        // below the stale held floor; the pump replays the gap idempotently.
        ack.register(&key("db", "a"), pos("binlog.000001", 650), false);
        assert_eq!(
            ack.committed(&key("db", "a")),
            Some(pos("binlog.000001", 650))
        );
        assert_eq!(
            ack.delivered(&key("db", "a")),
            Some(pos("binlog.000001", 650))
        );
        assert!(
            !ack.is_streaming(&key("db", "a")),
            "rejoined member is held until promoted"
        );
    }
}
