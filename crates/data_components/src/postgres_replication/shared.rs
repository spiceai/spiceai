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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError, RwLock};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationEvent, TryRecvEvent};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use bytes::Bytes;

use super::{
    Error, ReplicationMetricsCollector, ReplicationStreamInput, Result, SchemaEvolutionPolicy,
    bootstrap, changes::PgChangeRows, client, config::ReplicationParams, pgoutput, resilience,
    schema_evolution::RelationSchemaTracker, slot,
};
use rustc_hash::FxHashMap;

use crate::cdc::{ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError};
use crate::postgres_replication::pgoutput::RelationId;

/// A resolved route for one relation, cached at `Relation` time. Bundling the
/// member handle with its ack slot keeps the hot per-commit ack path
/// (`already_committed` / `deliver` / committer construction) off the
/// [`AckTable`] lock entirely — a pointer clone, no key hash or map lookup.
struct Route {
    key: MemberKey,
    member: Arc<MemberHandle>,
    slot: Arc<AckSlot>,
    /// The member's *current* working schema: its registered schema plus any
    /// mid-stream widening adopted by the per-member [`RelationSchemaTracker`]
    /// (see [`handle_relation`]). Under [`SchemaEvolutionPolicy::Block`] this is
    /// just the registered `member.schema`. `deliver_commit` builds each
    /// `PgChangeRows` against this so an adopted column reaches the accelerator.
    working_schema: SchemaRef,
}

/// Per-connection routing table: relation id -> resolved [`Route`]. `FxHashMap`
/// (not the `SipHash` default) since the keys are trusted internal relation ids
/// and this is on the per-event pump hot path — a `u32` `FxHash` is ~1-2ns vs
/// `SipHash`'s ~10-20ns, and bit-mixing keeps `hashbrown`'s SIMD filter
/// effective (unlike an identity `nohash`, whose zero high bits defeat it as the
/// map grows).
type RouteMap = FxHashMap<RelationId, Route>;

/// Per-transaction buffer of raw pgoutput change bytes, keyed by relation id.
/// `FxHashMap` for the same hot-path reason as [`RouteMap`].
type TxnBuffer = FxHashMap<RelationId, Vec<Bytes>>;

/// Pump-local, per-member schema-evolution state. Held in a map that PERSISTS
/// across reconnects (see `run_pump`, matching the former dedicated path): an
/// adopted mid-stream column add / lossless widening must survive a transient
/// disconnect, or the first `Relation` after reconnect (a tracker "first
/// observation") would fail to re-adopt it and silently drop the column's
/// values. A removed member's entry is cleared in [`handle_relation`].
#[derive(Default)]
struct MemberSchemaState {
    /// Adopts mid-stream widening under a non-[`SchemaEvolutionPolicy::Block`]
    /// policy; `None` under `Block`, where the working schema is fixed.
    tracker: Option<RelationSchemaTracker>,
    /// `Block`-mode observability only: the source column set last seen, so a
    /// mid-stream column add is warned about exactly once (under `Block` the
    /// new column is dropped because the working schema stays fixed).
    known_columns: Option<HashSet<String>>,
    /// The member registration this state was seeded from. Because the state
    /// persists across reconnects and is keyed only by source `(schema, table)`,
    /// a detach + re-subscribe for the same table with a *different* registered
    /// schema / policy / primary keys (e.g. a config reload while the shared
    /// pump keeps running) would otherwise reuse a tracker built on stale
    /// assumptions and mis-shape the `ChangeBatch`. [`handle_relation`] rebuilds
    /// the state whenever this no longer matches the current member.
    seed: Option<MemberSeed>,
}

/// Fingerprint of the member registration a [`MemberSchemaState`] was built for.
struct MemberSeed {
    dataset_name: String,
    schema: SchemaRef,
    policy: SchemaEvolutionPolicy,
    primary_keys: Vec<String>,
}

impl MemberSeed {
    fn of(member: &MemberHandle) -> Self {
        Self {
            dataset_name: member.dataset_name.clone(),
            schema: Arc::clone(&member.schema),
            policy: member.policy,
            primary_keys: member.primary_keys.clone(),
        }
    }

    fn matches(&self, member: &MemberHandle) -> bool {
        // `dataset_name` is included so a rename (same source table + schema)
        // rebuilds the tracker — it embeds the dataset name in its warnings, so a
        // reused tracker would misattribute schema-evolution logs to the old name.
        self.dataset_name == member.dataset_name
            && self.policy == member.policy
            && self.primary_keys == member.primary_keys
            && self.schema == member.schema
    }
}

/// Pump-local map of per-member schema state, keyed like [`RouteMap`].
type MemberSchemaStates = FxHashMap<MemberKey, MemberSchemaState>;

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

/// Poison-shrugging read/write helpers for the [`AckTable`] `RwLock`, same
/// rationale as [`lock`]: every critical section is a short read-modify-write
/// over plain data behind refcounted slots, so a panicking peer cannot leave the
/// map logically broken.
fn read_lock<T>(m: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_lock<T>(m: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(PoisonError::into_inner)
}

struct MemberHandle {
    dataset_name: String,
    schema: SchemaRef,
    primary_keys: Vec<String>,
    /// `GENERATED` columns of the member's source table — absent from
    /// pgoutput `Relation` messages by Postgres design; tolerated during
    /// schema validation and applied as NULL.
    generated_columns: Vec<String>,
    /// The dataset's `on_schema_change` policy. Drives the per-member
    /// [`RelationSchemaTracker`] in [`handle_relation`]: under a non-`Block`
    /// policy a mid-stream source column add / lossless type widening is adopted
    /// into the member's working schema (the runtime apply loop then reconciles
    /// the wider batch against the accelerator). Under `Block` the schema is
    /// fixed and a new source column is validated-then-dropped with a warning.
    policy: SchemaEvolutionPolicy,
    sender: mpsc::Sender<std::result::Result<ChangeEnvelope, StreamError>>,
    metrics: Arc<ReplicationMetricsCollector>,
    /// Lag-based readiness threshold for this member's dataset. WAL envelopes
    /// and idle keepalive heartbeats are flagged `is_dataset_ready` when their
    /// source-commit time is within this of now, so the dataset becomes Ready
    /// only once it has caught up to the source head.
    ready_lag: std::time::Duration,
}

/// `LIVE`: the member is attached and routing to it is allowed. Cleared on
/// detach, at which point its `committed` becomes a held floor the shared ack
/// can never pass.
const LIVE: u8 = 0b001;
/// `SNAPSHOTTING`: the member's initial snapshot is still running. Cleared by
/// the snapshot-completion hook; a member that detaches with this still set has
/// an accelerator missing base rows.
const SNAPSHOTTING: u8 = 0b010;
/// `STREAMING`: the pump has (re)connected at or below this member's floor, so
/// it is now routed and creditable. Members are *held* (no routing, never
/// credited, floor pinned) from registration until this promotion: crediting a
/// member before a connection provably covers its gap would acknowledge WAL it
/// never received (changes decoded while it had no route), losing them
/// permanently. See [`AckTable::promote_ready_members`].
const STREAMING: u8 = 0b100;

/// Per-member LSN accounting — one cache-line-isolated slot per member. The
/// consumer commit path (the hot ~5.5M/run write) is a single lock-free atomic
/// advance on the member's own `committed`, never a shared mutex; the pump
/// advances `delivered` (and, for idle members, both — see
/// [`AckTable::credit_idle`]). During streaming both fields advance monotonically,
/// so a torn pair-read across a concurrent writer can only under-observe, never
/// invent a value that was never written. The one non-monotonic step is a
/// deliberate rejoin reset: [`AckTable::register`] lowers `delivered` back to the
/// held `committed` floor, but only under the table write lock while the member
/// is detached (not streaming), so no concurrent pump/committer read races it.
///
/// `#[repr(align(64))]` isolates each member's slot on its own cache line: with
/// up to ~8 members committing on different consumer threads, adjacent
/// `committed` writes would otherwise false-share one line. Load-bearing — do
/// not "clean up".
#[repr(align(64))]
struct AckSlot {
    /// Highest commit LSN durably applied by this member's sink.
    committed: AtomicU64,
    /// Highest commit LSN delivered into this member's channel.
    delivered: AtomicU64,
    /// [`LIVE`] | [`SNAPSHOTTING`] | [`STREAMING`] bitflags. Mutated only under
    /// the [`AckTable`] write lock (register/detach/promote/snapshot-finished);
    /// read lock-free everywhere else.
    state: AtomicU8,
}

impl AckSlot {
    fn new(at: u64, snapshotting: bool) -> Self {
        Self {
            committed: AtomicU64::new(at),
            delivered: AtomicU64::new(at),
            state: AtomicU8::new(LIVE | if snapshotting { SNAPSHOTTING } else { 0 }),
        }
    }

    fn committed(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Acquire)
    }

    /// Advance this member's committed floor (monotonic). Called lock-free from
    /// the consumer commit path via [`SharedLsnCommitter`].
    fn commit(&self, lsn: u64) {
        advance_monotonic(&self.committed, lsn);
    }

    /// Record an envelope delivered into this member's channel (monotonic).
    /// The pump is the only caller; the CAS loop is kept for uniformity.
    fn deliver(&self, lsn: u64) {
        advance_monotonic(&self.delivered, lsn);
    }

    /// Whether the member has already durably applied this commit — used to
    /// suppress re-delivery of envelopes during a reconnect replay (the replay
    /// always starts at the *minimum* floor, so caught-up members would
    /// otherwise see every commit since the slowest member's position again).
    fn already_committed(&self, lsn: u64) -> bool {
        self.committed.load(Ordering::Acquire) >= lsn
    }

    fn has(&self, flag: u8) -> bool {
        self.state.load(Ordering::Acquire) & flag != 0
    }
}

/// Per-member LSN accounting for a shared slot. The slot-level acknowledgment
/// (`shared_flush`) is the minimum `committed` over **all** members — live or
/// held — advanced monotonically and recomputed *lazily on read*
/// ([`Self::flush_lsn`]), not eagerly on every commit.
///
/// `members` is read-locked for the hot per-boundary sweeps (`credit_idle`,
/// `flush_lsn`), the `slot` lookup that caches a member's `Arc<AckSlot>` into the
/// route map, and the lock-free slot mutations they drive; the write lock is
/// taken only for the rare
/// structural/state transitions (register, detach, promote, snapshot-finished),
/// so in steady state the read path is an uncontended atomic. The per-member
/// `committed`/`delivered` atomics live behind the `Arc<AckSlot>`, so a committer
/// advances its floor without touching this lock at all.
#[derive(Default)]
struct AckTable {
    members: RwLock<HashMap<MemberKey, Arc<AckSlot>>>,
    shared_flush: AtomicU64,
}

impl AckTable {
    /// Register a member (or revive a detached one) in the *held* state. A
    /// rejoining member keeps its held `committed` floor — everything after
    /// it is about to be replayed.
    fn register(&self, key: &MemberKey, snapshotting: bool) {
        let at = self.flush_lsn();
        let held = LIVE | if snapshotting { SNAPSHOTTING } else { 0 };
        let mut members = write_lock(&self.members);
        match members.get(key) {
            Some(slot) => {
                // Rejoin: keep the held `committed` floor, reset `delivered` to
                // it (everything after is about to be replayed), and drop back
                // to the held (non-streaming) state.
                slot.delivered.store(slot.committed(), Ordering::Release);
                slot.state.store(held, Ordering::Release);
            }
            None => {
                members.insert(key.clone(), Arc::new(AckSlot::new(at, snapshotting)));
            }
        }
    }

    /// The member's initial snapshot finished cleanly. It stays *held* until
    /// the pump's next (re)connect promotes it — the caller must also request
    /// that reconnect.
    fn snapshot_finished(&self, key: &MemberKey) {
        if let Some(slot) = write_lock(&self.members).get(key) {
            slot.state.fetch_and(!SNAPSHOTTING, Ordering::AcqRel);
        }
    }

    /// Called by the pump immediately after a successful connect, whose
    /// `start_lsn` was the floor (min over all `committed`, held members
    /// included): every held, snapshot-complete member's gap is covered by
    /// this connection's replay, so they become routable and creditable.
    fn promote_ready_members(&self) {
        for slot in write_lock(&self.members).values() {
            let s = slot.state.load(Ordering::Acquire);
            if s & LIVE != 0 && s & SNAPSHOTTING == 0 {
                slot.state.fetch_or(STREAMING, Ordering::AcqRel);
            }
        }
    }

    /// Resolve a member's slot so the per-connection route map can cache it,
    /// keeping the hot commit/deliver paths off this lock entirely. The pump
    /// then reads streaming/committed state straight off the cached
    /// [`AckSlot`] — no further table lookups.
    fn slot(&self, key: &MemberKey) -> Option<Arc<AckSlot>> {
        read_lock(&self.members).get(key).map(Arc::clone)
    }

    /// Credit streaming members with no in-flight envelopes up to `upto` —
    /// the connection's in-order replay guarantees their routed changes below
    /// `upto` were already delivered. Detached members are never credited
    /// (that's the point of the hold), and neither are held (not-yet-promoted)
    /// members — they have no route yet, so "no in-flight envelopes" says
    /// nothing about what they've missed.
    fn credit_idle(&self, upto: u64) {
        for slot in read_lock(&self.members).values() {
            let s = slot.state.load(Ordering::Acquire);
            if s & (LIVE | STREAMING) == (LIVE | STREAMING) && slot.delivered() == slot.committed()
            {
                // Advance `committed` BEFORE `delivered` (both Release): a torn
                // observation must never show `delivered > committed`. Writing
                // committed first means a concurrent sweep/credit can only
                // under-credit (skip one opportunity, corrected at the next
                // keepalive), never credit past an in-flight envelope. This is
                // the one memory-ordering subtlety here; any future edit MUST
                // preserve the order.
                advance_monotonic(&slot.committed, upto);
                advance_monotonic(&slot.delivered, upto);
            }
        }
    }

    /// Detach a member, returning whether it was still snapshotting (its
    /// snapshot never completed cleanly). The slot stays in the map with its
    /// `committed` frozen as a held floor — WAL retention by design.
    fn detach(&self, key: &MemberKey) -> bool {
        match write_lock(&self.members).get(key) {
            Some(slot) => {
                let prev = slot.state.fetch_and(!(LIVE | STREAMING), Ordering::AcqRel);
                prev & SNAPSHOTTING != 0
            }
            None => false,
        }
    }

    /// Seed the shared flush with the slot's consistent LSN so we never ack 0.
    fn seed(&self, lsn: u64) {
        advance_monotonic(&self.shared_flush, lsn);
    }

    /// Read-time slot-level floor: the minimum `committed` over all members
    /// (held members included — their frozen floor pins WAL by design), advanced
    /// monotonically into `shared_flush`. Recomputed on read, so nobody should
    /// cache the result expecting eager freshness. The monotonic CAS preserves
    /// the never-regress guarantee even if a `register` seeds a slot below the
    /// last reported floor mid-sweep. The sweep is ≤ 8 atomic loads under an
    /// uncontended read lock — it replaces the old per-commit `recompute`.
    fn flush_lsn(&self) -> u64 {
        let floor = read_lock(&self.members)
            .values()
            .map(|slot| slot.committed())
            .min();
        if let Some(floor) = floor {
            advance_monotonic(&self.shared_flush, floor);
        }
        self.shared_flush.load(Ordering::Acquire)
    }

    /// Whether `key`'s member has been promoted to the streaming phase (past its
    /// initial snapshot). Cold-path only (idle-heartbeat readiness gating); the
    /// hot commit/deliver paths read `STREAMING` off the cached `AckSlot`.
    fn is_streaming(&self, key: &MemberKey) -> bool {
        self.slot(key).is_some_and(|slot| slot.has(STREAMING))
    }
}

/// Key-addressed conveniences for tests, mirroring the production paths that
/// operate on a resolved [`AckSlot`] (the committer holds its slot; the pump
/// caches it in the route map). Kept test-only so production never pays the
/// key→slot lookup on the hot path.
#[cfg(test)]
impl AckTable {
    fn commit(&self, key: &MemberKey, lsn: u64) {
        if let Some(slot) = self.slot(key) {
            slot.commit(lsn);
        }
    }

    fn deliver(&self, key: &MemberKey, lsn: u64) {
        if let Some(slot) = self.slot(key) {
            slot.deliver(lsn);
        }
    }

    fn committed(&self, key: &MemberKey) -> u64 {
        self.slot(key).map_or(0, |slot| slot.committed())
    }

    fn delivered(&self, key: &MemberKey) -> u64 {
        self.slot(key).map_or(0, |slot| slot.delivered())
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
/// committed LSN via its own cache-line-isolated [`AckSlot`] — a single
/// lock-free atomic, no shared mutex and no floor recompute (the floor is read
/// lazily at the next boundary by [`AckTable::flush_lsn`]).
struct SharedLsnCommitter {
    slot: Arc<AckSlot>,
    flush_to: u64,
    /// Member dataset name, for the committer-progress log line.
    dataset: String,
    /// Source-commit timestamp (ms since the Unix epoch) of the batch this
    /// commit acks; `None` when the transaction carried no commit time.
    source_commit_ts_ms: Option<i64>,
}

#[async_trait]
impl CommitChange for SharedLsnCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.slot.commit(self.flush_to);
        crate::cdc::log_committer_progress(
            "postgres",
            &self.dataset,
            &format!("lsn={}", self.flush_to),
            self.source_commit_ts_ms,
        );
        Ok(())
    }

    /// Same argument as the per-dataset `LsnCommitter`: the slot retains WAL
    /// until the shared floor advances past this commit, so a crash before a
    /// deferred commit re-streams the un-acked tail. Safe to defer.
    fn supports_deferral(&self) -> bool {
        true
    }

    /// Fold another shared-slot commit that targets the *same* member slot into
    /// this one by keeping the higher LSN. Sound because [`Self::commit`] is a
    /// monotonic `max` into the slot's `committed` — infallible and
    /// order-insensitive, so folding N consecutive commits to their max is
    /// identical to running them in order. A different slot (or a non-shared
    /// committer) is refused, so a mixed run never coalesces across members.
    fn try_absorb(&mut self, other: &dyn CommitChange) -> bool {
        match other
            .as_any()
            .and_then(|a| a.downcast_ref::<SharedLsnCommitter>())
        {
            Some(other) if Arc::ptr_eq(&self.slot, &other.slot) => {
                self.flush_to = self.flush_to.max(other.flush_to);
                true
            }
            _ => false,
        }
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
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
        policy,
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
            policy,
            sender,
            metrics: Arc::clone(&metrics),
            ready_lag: params.ready_lag,
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
        // Readiness is lag-based: a resuming member becomes Ready via lag-gated
        // WAL envelopes and the pump's keepalive heartbeats (see `deliver_commit`
        // and `run_pump`'s KeepAlive handling), not an immediate resume-time
        // ready signal that could mark a still-behind member Ready.
        Box::pin(stream::empty::<
            std::result::Result<ChangeEnvelope, StreamError>,
        >())
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
    // Throttle idle-heartbeat fan-out: keepalives arrive in bursts (one per
    // chunk of filtered/unrelated WAL the slot decodes), so emit at most one
    // heartbeat round per `heartbeat_every`. The per-keepalive `credit_idle`
    // ACK is unaffected. Members share the slot's connection params, so the
    // interval is derived from the source's ready_lag.
    let heartbeat_every = crate::cdc::heartbeat_interval(params.ready_lag);
    let mut last_heartbeat_at: Option<std::time::Instant> = None;
    // When the stream dropped (set as the inner loop breaks to reconnect); consumed
    // on the next successful connect to attribute the disconnected duration.
    let mut disconnect_at: Option<std::time::Instant> = None;
    // Per-member schema-evolution state. PERSISTS across reconnects (declared
    // OUTSIDE the reconnect loop, matching the former dedicated path). A
    // mid-stream column add / lossless widening is adopted only on the *second*
    // `Relation` for a member — the first is the baseline. If this were rebuilt
    // per reconnect, the first `Relation` after a transient disconnect would be
    // a fresh "first observation" and would NOT re-adopt an already-adopted
    // column, silently dropping its values until the next schema change. Keeping
    // the tracker (and its widened working schema) across reconnects means
    // `handle_relation` reseeds each rebuilt route's `working_schema` from it, so
    // an adopted column survives a reconnect; pre-evolution WAL replayed after a
    // reconnect null-fills the (nullable) added column correctly. `routes` is
    // still rebuilt per connection; a removed member's entry is cleared in
    // `handle_relation`.
    let mut schema_state: MemberSchemaStates = MemberSchemaStates::default();

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
                                    handle_relation(
                                        &source,
                                        &mut decoder,
                                        &mut routes,
                                        &mut schema_state,
                                        rel,
                                    )
                                    .await;
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
                ReplicationEvent::KeepAlive {
                    wal_end,
                    server_time_micros,
                    ..
                } => {
                    // Accumulate the server WAL end; the per-member fan-out
                    // (server_wal_end + confirmed_flush) happens once in the
                    // consolidated boundary flush below, not inline per event.
                    max_wal_end = max_wal_end.max(wal_end.0);
                    should_flush = true;
                    if !txn_open {
                        source.ack.credit_idle(wal_end.0);
                        // Idle heartbeat for lag-based readiness. `!txn_open`
                        // means the pump has caught up to the source head, so a
                        // streaming member with a drained channel is caught up
                        // too. Fan out a zero-row heartbeat stamped with the
                        // source-attested keepalive clock via NON-BLOCKING
                        // try_send: a member whose bounded queue is full is
                        // behind, so dropping its heartbeat is both harmless (a
                        // later keepalive re-sends) and correct, and — the fix
                        // for the reverted #11554 heartbeat regression — a slow
                        // member can never block this fan-out and starve another
                        // member's Ready signal. Only streaming members (promoted
                        // past their snapshot) are eligible; a still-snapshotting
                        // member has not caught up. `server_time_micros` is
                        // Postgres-epoch microseconds; 0 marks a synthetic
                        // keepalive, which we skip.
                        if server_time_micros > 0
                            && last_heartbeat_at.is_none_or(|at| at.elapsed() >= heartbeat_every)
                        {
                            last_heartbeat_at = Some(std::time::Instant::now());
                            let heartbeat_ts_ms =
                                client::pg_epoch_to_system_time(server_time_micros)
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .ok()
                                    .and_then(|d| i64::try_from(d.as_millis()).ok());
                            for (member_key, member) in source.live_members() {
                                if !source.ack.is_streaming(&member_key) {
                                    continue;
                                }
                                let is_ready = crate::cdc::source_commit_within_ready_lag(
                                    heartbeat_ts_ms,
                                    member.ready_lag,
                                );
                                // Build the heartbeat against the member's CURRENT
                                // working schema — which may have widened under
                                // non-`Block` schema evolution (see
                                // `handle_relation`) — so an idle heartbeat carries
                                // the same schema `deliver_commit` builds data
                                // batches against, never a stale narrower one.
                                let heartbeat_schema = schema_state
                                    .get(&member_key)
                                    .and_then(|s| s.tracker.as_ref())
                                    .map_or_else(
                                        || Arc::clone(&member.schema),
                                        |t| Arc::clone(t.working_schema()),
                                    );
                                match crate::cdc::build_heartbeat_envelope(
                                    &heartbeat_schema,
                                    heartbeat_ts_ms,
                                    is_ready,
                                ) {
                                    Ok(heartbeat) => {
                                        // Log the idle heartbeat (per member) so
                                        // lag-based readiness can be verified from
                                        // the logs (target spice_cdc::heartbeat).
                                        let heartbeat_lag_ms =
                                            crate::cdc::replication_lag_ms(heartbeat_ts_ms);
                                        tracing::debug!(
                                            target: "spice_cdc::heartbeat",
                                            connector = "postgres",
                                            dataset = %member.dataset_name,
                                            source_commit_ts_ms = ?heartbeat_ts_ms,
                                            is_dataset_ready = is_ready,
                                            lag_ms = ?heartbeat_lag_ms,
                                            "CDC idle heartbeat emitted"
                                        );
                                        // Drop-if-full / drop-if-closed: never
                                        // block the pump on a slow member.
                                        let _ = member.sender.try_send(Ok(heartbeat));
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            dataset = %member.dataset_name,
                                            error = %e,
                                            "failed to build shared Postgres CDC heartbeat envelope; skipping"
                                        );
                                    }
                                }
                            }
                        }
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
    schema_state: &mut MemberSchemaStates,
    rel: pgoutput::Relation,
) {
    let member_key: MemberKey = (rel.namespace.clone(), rel.name.clone());
    let Some(member) = source.member(&member_key) else {
        // No member for this table (e.g. publication membership left over from
        // a removed dataset). Its changes are dropped (never routed).
        routes.remove(&rel.relation_id);
        schema_state.remove(&member_key);
        tracing::debug!(
            table = %format_member(&member_key),
            slot = %source.key.slot_name,
            "relation in shared publication has no subscribed dataset; ignoring its changes"
        );
        return;
    };
    // Resolve the member's ack slot once (reused for the streaming gate below and
    // cached in the route), so the per-commit ack path is a pointer, not a lock.
    let Some(slot) = source.ack.slot(&member_key) else {
        routes.remove(&rel.relation_id);
        return;
    };
    if !slot.has(STREAMING) {
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

    // Reconcile the Relation against the member's working schema, mirroring the
    // former per-dataset path so slot-consolidated datasets keep their schema
    // evolution. Under `block` the schema is fixed (validate, then warn if the
    // source added a column whose values are being dropped). Otherwise a
    // per-member `RelationSchemaTracker` adopts a mid-stream column add / lossless
    // type widening into the working schema, so the built `ChangeBatch` carries
    // the wider data struct — the runtime apply loop then reconciles it against
    // the accelerator per the policy. `schema_state` persists across reconnects
    // (declared outside the reconnect loop in `run_pump`), so an adopted column
    // survives a transient disconnect rather than being dropped as a fresh
    // "first observation".
    let state = schema_state.entry(member_key.clone()).or_default();
    // Guard the persistence against a re-subscribe: if this member was
    // re-registered for the same source table with a different schema / policy /
    // primary keys (config reload), the persisted tracker's assumptions are
    // stale — rebuild the state from the current registration so it never
    // mis-shapes the `ChangeBatch`.
    if !state.seed.as_ref().is_some_and(|s| s.matches(&member)) {
        *state = MemberSchemaState {
            seed: Some(MemberSeed::of(&member)),
            ..MemberSchemaState::default()
        };
    }
    let working_schema = if member.policy == SchemaEvolutionPolicy::Block {
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
        // Observability-only (behavior unchanged under `block`): a mid-stream
        // column add is silently dropped — say so loudly once per change.
        client::warn_on_new_relation_columns(
            &rel,
            &mut state.known_columns,
            &member.dataset_name,
            &member.metrics,
        );
        Arc::clone(&member.schema)
    } else {
        // PK columns must exist and be part of the replica identity for every
        // policy — UPDATE/DELETE cannot route without them. (The tracker does
        // not re-check this.)
        if let Err(e) = client::validate_relation_primary_keys(&rel, &member.primary_keys) {
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
        let tracker = state.tracker.get_or_insert_with(|| {
            RelationSchemaTracker::new(
                Arc::clone(&member.schema),
                member.policy,
                member.dataset_name.clone(),
                member.primary_keys.clone(),
            )
        });
        match tracker.observe_relation(&rel) {
            Ok(observation) => {
                let widened = observation.schema_changed;
                let working = Arc::clone(tracker.working_schema());
                if widened {
                    member.metrics.inc_schema_evolution();
                    tracing::info!(
                        dataset = %member.dataset_name,
                        "adopted source schema change: {}",
                        observation.summary
                    );
                }
                working
            }
            Err(e) => {
                member.metrics.inc_schema_evolution_rejected();
                member.metrics.inc_schema_mismatch_error();
                routes.remove(&rel.relation_id);
                member_fatal(
                    source,
                    &member_key,
                    format!(
                        "schema change for {} cannot be applied: {e}",
                        member.dataset_name
                    ),
                )
                .await;
                return;
            }
        }
    };

    decoder.apply_declared_primary_keys(rel.relation_id, &member.primary_keys);
    // Cache the resolved handle + ack slot + current working schema alongside the
    // key so the per-event path skips the `members` lock + string hash (see
    // `buffer_raw_change`) and `deliver_commit` builds against the working schema.
    routes.insert(
        rel.relation_id,
        Route {
            key: member_key,
            member,
            slot,
            working_schema,
        },
    );
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
    let Some(Route { member, .. }) = routes.get(&relation_id) else {
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
        if let Some(Route { member, .. }) = routes.get(&relation_id) {
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
                // Log the cumulative wait for THIS delivery, not the constant
                // poll interval: a monotonically growing value distinguishes
                // one long continuous stall from scattered brief ones.
                tracing::warn!(
                    dataset = %dataset_name,
                    stalled_for = ?send_start.elapsed(),
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
        let Some(Route {
            key: member_key,
            member,
            slot,
            working_schema,
        }) = routes.get(&relation_id)
        else {
            continue;
        };
        if !slot.has(STREAMING) {
            continue;
        }
        if slot.already_committed(end_lsn) {
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
        // Build against the member's *working* schema (registered schema plus any
        // adopted mid-stream widening — see `handle_relation`), not the fixed
        // registered schema, so an adopted column reaches the accelerator.
        let rows = PgChangeRows::new(Arc::clone(working_schema), rel.clone(), raw, commit_ts_ms);
        member.metrics.inc_transaction();
        // Lag-based readiness: this WAL envelope marks the dataset Ready only if
        // its source commit time is within the member's `ready_lag` of now, i.e.
        // the member has caught up to the source head. A backlog (post-snapshot
        // gap replay, resume catch-up) keeps the dataset not-ready until closed.
        let is_ready = crate::cdc::source_commit_within_ready_lag(commit_ts_ms, member.ready_lag);
        let envelope = ChangeEnvelope::new_from_rows(
            Box::new(SharedLsnCommitter {
                slot: Arc::clone(slot),
                flush_to: end_lsn,
                dataset: member.dataset_name.clone(),
                source_commit_ts_ms: commit_ts_ms,
            }),
            Box::new(rows),
            is_ready,
        );
        slot.deliver(end_lsn);
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
            ready_lag: crate::cdc::DEFAULT_READY_LAG,
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
                    policy: SchemaEvolutionPolicy::Block,
                    sender,
                    metrics: Arc::clone(&metrics),
                    ready_lag: crate::cdc::DEFAULT_READY_LAG,
                }),
            );
            probes.push((member_key, metrics, receiver));
        }
        (source, probes)
    }

    /// The ported schema-evolution wiring: under a non-`Block` policy, a
    /// mid-stream source column add must be adopted into the member's working
    /// schema (which `deliver_commit` then builds the `ChangeBatch` against), so
    /// the runtime evolution layer sees the wider batch. A new column is adopted
    /// only on the *second* Relation (the first establishes the baseline), so
    /// this drives a baseline Relation then an ALTER-widened one.
    #[tokio::test]
    async fn handle_relation_adopts_mid_stream_column_add_into_working_schema() {
        use crate::postgres_replication::pgoutput::{Column, Relation};
        use arrow::datatypes::{DataType, Field, Schema};

        let source = Arc::new(SharedSource::new(
            SourceKey::from_params(&test_params()),
            test_params(),
        ));
        let member_key: MemberKey = ("public".to_string(), "users".to_string());
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let (sender, _rx) = mpsc::channel(4);
        lock(&source.members).insert(
            member_key.clone(),
            Arc::new(MemberHandle {
                dataset_name: "users".into(),
                schema,
                primary_keys: vec!["id".into()],
                generated_columns: vec![],
                policy: SchemaEvolutionPolicy::AppendNewColumns,
                sender,
                metrics: ReplicationMetricsCollector::new(),
                ready_lag: crate::cdc::DEFAULT_READY_LAG,
            }),
        );
        source.ack.register(&member_key, false);
        source.ack.promote_ready_members();

        let mut decoder = pgoutput::Decoder::new();
        let mut routes = RouteMap::default();
        let mut schema_state = MemberSchemaStates::default();

        let id_col = || Column {
            is_key: true,
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
        };
        let rel = |cols: Vec<Column>| Relation {
            relation_id: 42,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'd',
            columns: cols,
        };
        let working = |routes: &RouteMap| -> Vec<String> {
            routes
                .get(&42)
                .expect("route built")
                .working_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };

        // Baseline Relation: working schema unchanged (id only).
        handle_relation(
            &source,
            &mut decoder,
            &mut routes,
            &mut schema_state,
            rel(vec![id_col()]),
        )
        .await;
        assert_eq!(working(&routes), vec!["id".to_string()]);

        // Mid-stream ALTER adds `name` (text): the shared pump must adopt it.
        let name_col = || Column {
            is_key: false,
            name: "name".into(),
            type_oid: 25,
            type_modifier: -1,
        };
        handle_relation(
            &source,
            &mut decoder,
            &mut routes,
            &mut schema_state,
            rel(vec![id_col(), name_col()]),
        )
        .await;
        assert_eq!(
            working(&routes),
            vec!["id".to_string(), "name".to_string()],
            "shared pump adopted the mid-stream column add under append_new_columns"
        );

        // Simulate a transient reconnect: the pump rebuilds `routes` per
        // connection but `schema_state` PERSISTS (declared outside the reconnect
        // loop in run_pump). The first Relation after reconnect already carries
        // `name` (the stream resumed past the ALTER) — a tracker "first
        // observation" that on its own would NOT adopt an added column. The
        // persisted tracker must keep the adoption so the column is not dropped.
        routes.clear();
        handle_relation(
            &source,
            &mut decoder,
            &mut routes,
            &mut schema_state,
            rel(vec![id_col(), name_col()]),
        )
        .await;
        assert_eq!(
            working(&routes),
            vec!["id".to_string(), "name".to_string()],
            "adopted column survives a reconnect because schema_state persists across the WAL gap"
        );
    }

    /// The persisted `schema_state` must not carry stale assumptions across a
    /// re-subscribe: if the same source table is re-registered with a different
    /// schema (config reload while the pump keeps running), the tracker is
    /// rebuilt from the new registration rather than reusing the old widened
    /// working schema.
    #[tokio::test]
    async fn handle_relation_rebuilds_state_on_resubscribe_with_changed_schema() {
        use crate::postgres_replication::pgoutput::{Column, Relation};
        use arrow::datatypes::{DataType, Field, Schema};

        let source = Arc::new(SharedSource::new(
            SourceKey::from_params(&test_params()),
            test_params(),
        ));
        let member_key: MemberKey = ("public".to_string(), "users".to_string());
        // Keep receivers alive so the members' senders stay open for the test.
        let mut keepalive = Vec::new();
        let mut register = |schema: SchemaRef| {
            let (sender, rx) = mpsc::channel(4);
            keepalive.push(rx);
            lock(&source.members).insert(
                member_key.clone(),
                Arc::new(MemberHandle {
                    dataset_name: "users".into(),
                    schema,
                    primary_keys: vec!["id".into()],
                    generated_columns: vec![],
                    policy: SchemaEvolutionPolicy::AppendNewColumns,
                    sender,
                    metrics: ReplicationMetricsCollector::new(),
                    ready_lag: crate::cdc::DEFAULT_READY_LAG,
                }),
            );
            source.ack.register(&member_key, false);
            source.ack.promote_ready_members();
        };

        let id_col = || Column {
            is_key: true,
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
        };
        let text_col = |name: &str| Column {
            is_key: false,
            name: name.into(),
            type_oid: 25,
            type_modifier: -1,
        };
        let rel = |cols: Vec<Column>| Relation {
            relation_id: 42,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'd',
            columns: cols,
        };
        let working = |routes: &RouteMap| -> Vec<String> {
            routes
                .get(&42)
                .expect("route built")
                .working_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };

        let mut decoder = pgoutput::Decoder::new();
        let mut routes = RouteMap::default();
        let mut schema_state = MemberSchemaStates::default();

        // First registration: schema [id]; adopt `name` mid-stream → [id, name].
        register(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )])));
        handle_relation(
            &source,
            &mut decoder,
            &mut routes,
            &mut schema_state,
            rel(vec![id_col()]),
        )
        .await;
        handle_relation(
            &source,
            &mut decoder,
            &mut routes,
            &mut schema_state,
            rel(vec![id_col(), text_col("name")]),
        )
        .await;
        assert_eq!(working(&routes), vec!["id".to_string(), "name".to_string()]);

        // Re-subscribe for the same table with a DIFFERENT registered schema
        // ([id, email]). The stale tracker (which had adopted `name`) must be
        // discarded and rebuilt from the new registration, so the first Relation
        // is a fresh baseline that yields exactly the new schema — not the old
        // widened [id, name] or a mash-up.
        register(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("email", DataType::Utf8, true),
        ])));
        handle_relation(
            &source,
            &mut decoder,
            &mut routes,
            &mut schema_state,
            rel(vec![id_col(), text_col("email")]),
        )
        .await;
        assert_eq!(
            working(&routes),
            vec!["id".to_string(), "email".to_string()],
            "re-subscribe with a new schema rebuilds the tracker; the stale `name` adoption is dropped"
        );
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
            slot: ack.slot(&key("a")).expect("slot registered"),
            flush_to: 42,
            dataset: "test".to_string(),
            source_commit_ts_ms: None,
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

    /// Seeded xorshift64* PRNG — zero-dependency and reproducible by seed, so a
    /// differential failure re-runs deterministically (§5.2). Not
    /// cryptographic; just needs a well-spread `u64` stream.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Reference model: a direct, deliberately naive reimplementation of the
    /// *pre-`AckSlot`* semantics (mutex-`HashMap` + eager `recompute` on every
    /// commit/credit, `flush_lsn` = the eagerly-maintained floor). The
    /// differential test (below) asserts the new lock-free `AckTable` is
    /// byte-identical to this on every operation.
    #[derive(Default)]
    struct Model {
        entries: HashMap<MemberKey, ModelEntry>,
        shared_flush: u64,
    }
    #[derive(Clone, Copy)]
    struct ModelEntry {
        committed: u64,
        delivered: u64,
        live: bool,
        snapshotting: bool,
        streaming: bool,
    }
    impl Model {
        fn seed(&mut self, lsn: u64) {
            self.shared_flush = self.shared_flush.max(lsn);
        }
        fn register(&mut self, key: &MemberKey, snapshotting: bool) {
            let at = self.shared_flush;
            self.entries
                .entry(key.clone())
                .and_modify(|e| {
                    e.live = true;
                    e.delivered = e.committed;
                    e.snapshotting = snapshotting;
                    e.streaming = false;
                })
                .or_insert(ModelEntry {
                    committed: at,
                    delivered: at,
                    live: true,
                    snapshotting,
                    streaming: false,
                });
        }
        fn snapshot_finished(&mut self, key: &MemberKey) {
            if let Some(e) = self.entries.get_mut(key) {
                e.snapshotting = false;
            }
        }
        fn promote_ready_members(&mut self) {
            for e in self.entries.values_mut() {
                if e.live && !e.snapshotting {
                    e.streaming = true;
                }
            }
        }
        fn deliver(&mut self, key: &MemberKey, lsn: u64) {
            if let Some(e) = self.entries.get_mut(key) {
                e.delivered = e.delivered.max(lsn);
            }
        }
        fn commit(&mut self, key: &MemberKey, lsn: u64) {
            if let Some(e) = self.entries.get_mut(key) {
                e.committed = e.committed.max(lsn);
            }
            self.recompute();
        }
        fn credit_idle(&mut self, upto: u64) {
            for e in self.entries.values_mut() {
                if e.live && e.streaming && e.delivered == e.committed {
                    let lsn = e.committed.max(upto);
                    e.committed = lsn;
                    e.delivered = lsn;
                }
            }
            self.recompute();
        }
        fn detach(&mut self, key: &MemberKey) {
            if let Some(e) = self.entries.get_mut(key) {
                e.live = false;
                e.streaming = false;
            }
        }
        fn recompute(&mut self) {
            if let Some(floor) = self.entries.values().map(|e| e.committed).min() {
                self.shared_flush = self.shared_flush.max(floor);
            }
        }
        fn flush_lsn(&self) -> u64 {
            self.shared_flush
        }
    }

    /// §5.2 — the main correctness instrument. Drive the new [`AckTable`] and the
    /// reference [`Model`] with the same randomized operation sequences and
    /// assert identical `flush_lsn` and per-member `committed`/`delivered` after
    /// every op. Single-threaded — this pins the *semantics*; concurrency is
    /// [`credit_idle_interleaving_never_over_credits`]'s job.
    #[test]
    fn differential_matches_reference_model() {
        const MEMBERS: u64 = 4;
        let members: Vec<MemberKey> = (0..MEMBERS).map(|i| key(&format!("t{i}"))).collect();
        // Seeded so a failure is reproducible; a spread of seeds covers more
        // interleavings while keeping the run cheap.
        for seed in 1..=400_u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let ack = AckTable::default();
            let mut model = Model::default();
            // Per-member monotonic LSN generators.
            let mut next_lsn = vec![100_u64; members.len()];
            let mut upto = 100_u64;

            ack.seed(100);
            model.seed(100);

            for _ in 0..800 {
                let m = usize::try_from(rng.below(MEMBERS)).expect("member index fits usize");
                let k = &members[m];
                match rng.below(7) {
                    0 => {
                        let snap = rng.below(2) == 0;
                        ack.register(k, snap);
                        model.register(k, snap);
                    }
                    1 => {
                        ack.promote_ready_members();
                        model.promote_ready_members();
                    }
                    2 => {
                        next_lsn[m] += rng.below(50) + 1;
                        ack.deliver(k, next_lsn[m]);
                        model.deliver(k, next_lsn[m]);
                    }
                    3 => {
                        // Commit at or below what has been delivered to this
                        // member (the real invariant: a commit never outruns
                        // delivery). Occasionally replay an older LSN.
                        let lsn = if rng.below(4) == 0 {
                            next_lsn[m].saturating_sub(rng.below(30))
                        } else {
                            next_lsn[m]
                        };
                        ack.commit(k, lsn);
                        model.commit(k, lsn);
                    }
                    4 => {
                        upto += rng.below(40) + 1;
                        ack.credit_idle(upto);
                        model.credit_idle(upto);
                    }
                    5 => {
                        ack.snapshot_finished(k);
                        model.snapshot_finished(k);
                    }
                    _ => {
                        ack.detach(k);
                        model.detach(k);
                    }
                }

                assert_eq!(
                    ack.flush_lsn(),
                    model.flush_lsn(),
                    "flush_lsn diverged (seed {seed})"
                );
                for (i, mk) in members.iter().enumerate() {
                    // Only compare registered members (both register in lockstep).
                    if model.entries.contains_key(mk) {
                        assert_eq!(
                            ack.committed(mk),
                            model.entries[mk].committed,
                            "committed[t{i}] diverged (seed {seed})"
                        );
                        assert_eq!(
                            ack.delivered(mk),
                            model.entries[mk].delivered,
                            "delivered[t{i}] diverged (seed {seed})"
                        );
                    }
                }
            }
        }
    }

    /// §5.3 — the `credit_idle` write-ordering invariant under real threads.
    /// One thread commits a member forward while another races `credit_idle`
    /// ahead of it; a third observer asserts the slot-level floor never runs
    /// past the member's own committed LSN, and everything converges to the
    /// final LSN once quiescent. Exercises the `committed`-before-`delivered`
    /// Release ordering against genuine cross-thread writes (run this in
    /// `--release` with a high count to shake out torn reads).
    #[test]
    fn credit_idle_interleaving_never_over_credits() {
        use std::sync::atomic::AtomicBool;

        const N: u64 = 20_000;
        let ack = Arc::new(AckTable::default());
        ack.seed(1);
        ack.register(&key("a"), false);
        ack.promote_ready_members();
        let slot = ack.slot(&key("a")).expect("slot");
        let done = Arc::new(AtomicBool::new(false));

        std::thread::scope(|s| {
            // Committer: deliver then commit, strictly increasing. Signals
            // `done` on completion so the crediter's busy loop terminates —
            // setting `done` after the scope would deadlock, since the scope
            // only joins once the crediter has already exited.
            {
                let slot = Arc::clone(&slot);
                let done = Arc::clone(&done);
                s.spawn(move || {
                    for lsn in 2..=N {
                        slot.deliver(lsn);
                        slot.commit(lsn);
                    }
                    done.store(true, Ordering::Release);
                });
            }
            // Idle-crediter: race `upto` ahead of the committer until it's done.
            {
                let ack = Arc::clone(&ack);
                let done = Arc::clone(&done);
                s.spawn(move || {
                    while !done.load(Ordering::Acquire) {
                        ack.credit_idle(N + 1000);
                    }
                });
            }
            // Observer: two continuously-checked invariants under contention —
            // (1) the slot-level floor never *exceeds* the member's own
            // committed LSN (acking past what a member durably applied is the
            // data-loss bug this whole design guards against), and (2) the floor
            // never regresses (contract #1). Read the floor *before* committed:
            // both are monotonic, so a later committed read can only be ≥ the
            // committed the floor was computed from — a floor that jumped past a
            // real in-flight commit is thus observable, not masked.
            {
                let ack = Arc::clone(&ack);
                let slot = Arc::clone(&slot);
                s.spawn(move || {
                    let mut last_floor = 0;
                    for _ in 0..200_000 {
                        let floor = ack.flush_lsn();
                        let committed = slot.committed();
                        assert!(
                            floor <= committed,
                            "floor {floor} ran past committed {committed}"
                        );
                        assert!(
                            floor >= last_floor,
                            "floor regressed {last_floor} -> {floor}"
                        );
                        last_floor = floor;
                    }
                });
            }
        });

        // Quiescence: `credit_idle` may legitimately have advanced an idle
        // member up to its `upto` (N+1000), so `committed` lands somewhere in
        // [N, N+1000]. What must hold is convergence: the floor equals the
        // member's committed (single member ⇒ the floor *is* its committed).
        let final_committed = slot.committed();
        assert!(
            (N..=N + 1000).contains(&final_committed),
            "committed {final_committed} outside any writer's range"
        );
        assert_eq!(
            ack.flush_lsn(),
            final_committed,
            "floor converged to the member's committed"
        );
    }
}
