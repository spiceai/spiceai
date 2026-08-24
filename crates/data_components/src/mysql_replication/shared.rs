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

//! Share one `MySQL` binlog dump connection across every
//! `refresh_mode: changes` dataset on a connection — the `MySQL` analog of
//! [`crate::postgres_replication::shared`], but **always on**.
//!
//! Unlike a Postgres publication, `MySQL`'s `COM_BINLOG_DUMP` has no
//! server-side table filter: every subscriber receives the *entire* server
//! binlog. A dedicated per-dataset dump would therefore just duplicate the
//! whole stream for no benefit, so there is no per-dataset path and no opt-in:
//! this module is the sole streaming engine for `MySQL` CDC. Sharing is keyed by
//! *connection identity* ([`SourceKey`]) — datasets that connect the same way
//! (host, port, user, password, TLS, `server_id`) join a single *shared source*
//! (one dump connection, one `server_id`), with decoded transactions routed by
//! `(database, table)` to each member's accelerator sink. The database is not
//! part of the key, so datasets on the same server but different databases still
//! share. A single dataset is simply a shared source with one member; datasets
//! that connect *differently* get their own source (see [`SourceKey`] for the
//! per-dataset opt-out seam).
//!
//! # Consistency & ack model
//!
//! An at-least-once contract, made convergent by
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
//! self-fatal rather than mis-decoded. Because a fetched layout describes the
//! source *now* rather than the event being decoded, every routed `TableMap` is
//! additionally checked against the column types the event itself carries
//! ([`super::binlog::layout_event_mismatch`]), which is what catches a
//! same-column-count reorder adopted from ahead of the stream. A durable
//! pre-adopt/replay-boundary checkpoint machinery is intentionally *not*
//! implemented in v1. Instead,
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
use mysql_async::binlog::events::{EventData, RotateEvent, RowsEventData, TableMapEvent};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::binlog::{
    AdoptedLayout, MIN_VALID_EVENT_POS, QueryKind, StatementKind, adopt_current_layout,
    classify_query, classify_statement, commit_ts_ms, compute_pk_source_indexes,
    layout_event_mismatch, log_transient_reconnect, open_binlog_stream, purged_position_error,
    readiness_heartbeat, record_watermark,
};
use super::changes::{MemberLayout, MysqlChangeRows};
use super::config::{BinlogPosition, ReplicationParams};
use super::metrics::MetricsCollector;
use super::rows::{build_change_batch, truncate_change};
use super::{
    CursorType, Error, GtidSet, PersistedPosition, PositionStore, ReplicationStreamInput, Result,
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

/// Identity of a shared dump: the connection a dataset streams the binlog over.
/// Every `refresh_mode: changes` dataset that connects the same way coalesces
/// onto one pump for this key — no opt-in, no group label. The `MySQL` binlog
/// dump is server-wide (no server-side table filter), so separate connections
/// to the same server would just duplicate the stream; coalescing is always the
/// right default. The database is deliberately NOT part of the key — datasets on
/// the same server but different databases still share the one server-wide dump.
///
/// The key captures everything that makes two connections "the same way":
/// host/port/user, password, and the full TLS config (`SslOpts`). Datasets that
/// connect *differently* (e.g. a different `sslmode`, a different credential)
/// simply produce a different key and get their own dump — so one can never
/// silently ride another's transport or credentials, and there is nothing to
/// reject at join time.
///
/// `server_id` is part of the key too: it defaults to a value derived from the
/// connection identity (so datasets on one connection coalesce), but a user who
/// sets *distinct* explicit `mysql_replication_server_id`s gets *distinct* keys
/// — i.e. separate dedicated dumps. That is the per-dataset opt-out seam, built
/// from existing config with no new mechanism and no second engine.
#[derive(Clone, PartialEq, Eq, Hash)]
struct SourceKey {
    host: String,
    port: u16,
    user: String,
    pass: Option<String>,
    ssl: Option<mysql_async::SslOpts>,
    server_id: u32,
}

// Manual, credential-redacting `Debug`: the key holds the password and full TLS
// material, so a derived `Debug` risks leaking secrets into logs or a panic
// message. Print only the safe connection label + `server_id`, and the mere
// presence (not value) of a password / TLS config. For log lines prefer
// [`SourceKey::label`].
impl std::fmt::Debug for SourceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceKey")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("pass", &self.pass.as_ref().map(|_| "<redacted>"))
            .field("ssl", &self.ssl.as_ref().map(|_| "<redacted>"))
            .field("server_id", &self.server_id)
            .finish()
    }
}

impl SourceKey {
    fn from_params(params: &ReplicationParams) -> Self {
        Self {
            host: params.opts.ip_or_hostname().to_string(),
            port: params.opts.tcp_port(),
            user: params.opts.user().unwrap_or_default().to_string(),
            pass: params.opts.pass().map(ToString::to_string),
            ssl: params.opts.ssl_opts().cloned(),
            server_id: params.server_id,
        }
    }

    /// Short human-readable label for logs/errors (`host:port`). Never includes
    /// the credential or TLS material.
    fn label(&self) -> String {
        format!("{}:{}", self.host, self.port)
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

/// Monotonic-max advance of a mutex-guarded binlog position — the shared logic
/// behind [`AckSlot::commit`] and [`AckSlot::deliver`].
fn advance_position(m: &Mutex<BinlogPosition>, to: &BinlogPosition) {
    let mut g = lock(m);
    if *to > *g {
        *g = to.clone();
    }
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
    /// Executed GTID set for this member, advanced in lockstep with `committed`
    /// (folded on commit and on idle-credit), so it describes exactly the
    /// transactions at or below the member's committed position. Empty when the
    /// source is not GTID-positioning. The shared dump's GTID resume set is the
    /// intersection of these across members (see [`AckTable::resume_gtid`]).
    gtid: Mutex<GtidSet>,
    state: AtomicU8,
}

impl AckSlot {
    fn new(at: BinlogPosition, gtid_seed: GtidSet, snapshotting: bool) -> Self {
        Self {
            committed: Mutex::new(at.clone()),
            delivered: Mutex::new(at),
            gtid: Mutex::new(gtid_seed),
            state: AtomicU8::new(LIVE | if snapshotting { SNAPSHOTTING } else { 0 }),
        }
    }

    fn committed(&self) -> BinlogPosition {
        lock(&self.committed).clone()
    }
    fn delivered(&self) -> BinlogPosition {
        lock(&self.delivered).clone()
    }

    /// Fold a committed transaction's GTID into this member's executed set — run
    /// as the member's committed position advances past the transaction, so the
    /// set stays exactly in step with the durable cursor.
    fn fold_gtid(&self, uuid: uuid::Uuid, gno: u64) {
        lock(&self.gtid).add(uuid, gno);
    }
    fn gtid_snapshot(&self) -> GtidSet {
        lock(&self.gtid).clone()
    }

    /// Advance this member's committed floor (monotonic-max).
    fn commit(&self, to: &BinlogPosition) {
        advance_position(&self.committed, to);
    }
    /// Record an envelope delivered into this member's channel (monotonic-max).
    fn deliver(&self, to: &BinlogPosition) {
        advance_position(&self.delivered, to);
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
    #[cfg(test)]
    fn register(&self, key: &MemberKey, at: BinlogPosition, snapshotting: bool) {
        self.register_with_gtid(key, at, GtidSet::new(), snapshotting);
    }

    /// [`Self::register`] carrying the resolved executed GTID seed (the source
    /// head's set on cold start, or the member's persisted set on resume). A
    /// reviving slot is RESET to `(at, gtid_seed)` — the sidecar is the source
    /// of truth, so a stale in-memory floor/set is discarded.
    fn register_with_gtid(
        &self,
        key: &MemberKey,
        at: BinlogPosition,
        gtid_seed: GtidSet,
        snapshotting: bool,
    ) {
        let held = LIVE | if snapshotting { SNAPSHOTTING } else { 0 };
        let mut members = write_lock(&self.members);
        match members.get(key) {
            Some(slot) => {
                *lock(&slot.committed) = at.clone();
                *lock(&slot.delivered) = at;
                *lock(&slot.gtid) = gtid_seed;
                slot.state.store(held, Ordering::Release);
            }
            None => {
                members.insert(
                    key.clone(),
                    Arc::new(AckSlot::new(at, gtid_seed, snapshotting)),
                );
            }
        }
    }

    /// The shared dump's GTID resume set: the intersection of every member's
    /// executed set — the GTIDs *all* members have applied. A
    /// `COM_BINLOG_DUMP_GTID` from it re-sends any transaction some member still
    /// needs (members ahead suppress via their committed floor), the GTID analog
    /// of [`Self::flush_position`]'s minimum. `None` when there are no members.
    fn resume_gtid(&self) -> Option<GtidSet> {
        let members = read_lock(&self.members);
        let mut iter = members.values();
        let first = iter.next()?.gtid_snapshot();
        Some(iter.fold(first, |acc, slot| acc.intersect(&slot.gtid_snapshot())))
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
    fn credit_idle(&self, upto: &BinlogPosition, gtid: Option<(uuid::Uuid, u64)>) {
        for slot in read_lock(&self.members).values() {
            let s = slot.state.load(Ordering::Acquire);
            if s & (LIVE | STREAMING) == (LIVE | STREAMING) && slot.delivered() == slot.committed()
            {
                slot.commit(upto);
                slot.deliver(upto);
                // An idle member's committed advances past this transaction, so
                // its executed set gains the transaction's GTID too (idempotent
                // — re-folding an already-present GTID is a no-op).
                if let Some((uuid, gno)) = gtid {
                    slot.fold_gtid(uuid, gno);
                }
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
    /// GTIDs of the transaction(s) this commit acks, folded into the member's
    /// executed set on commit (in lockstep with `flush_to`) for failover-safe
    /// resume. A `Vec` — not a single GTID — because [`Self::try_absorb`] may
    /// coalesce several transactions' commits into one, and every one of their
    /// GTIDs must still be folded. Empty when the source is not GTID-positioning.
    gtids: Vec<(uuid::Uuid, u64)>,
}

#[async_trait]
impl CommitChange for SharedPositionCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.slot.commit(&self.flush_to);
        for &(uuid, gno) in &self.gtids {
            self.slot.fold_gtid(uuid, gno);
        }
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
                // Every coalesced transaction's GTID must still fold, regardless
                // of which commit has the higher position.
                self.gtids.extend_from_slice(&other.gtids);
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
        // Persist the captured head now that the snapshot is durably applied —
        // the same contract as the per-dataset path's boundary committer.
        // `persist_all` skips SNAPSHOTTING members, so without this the head is
        // not persisted until the pump's next checkpoint tick; a crash in that
        // window would needlessly re-snapshot. Running here (after every
        // snapshot batch has been applied) makes the head durable immediately.
        if let (Some(member), Some(slot)) = (
            self.source.member(&self.key),
            self.source.ack.slot(&self.key),
        ) {
            let persisted = persisted_for(&member, &slot);
            if let Err(e) = member.position_store.save(&persisted).await {
                tracing::warn!(dataset = %self.dataset, error = %e, "failed to persist shared mysql binlog snapshot head");
            }
        }
        self.source.restart_requested.store(true, Ordering::Release);
        crate::cdc::log_committer_progress("mysql", &self.dataset, "snapshot-complete", None);
        Ok(())
    }
}

/// The zero-row boundary envelope carrying a [`SnapshotBoundaryCommitter`]:
/// promotes the member out of `SNAPSHOTTING`, persists the captured head, and
/// requests the reconnect — once the consumer has durably applied everything
/// before it.
///
/// `history_unavailable` additionally asks the consumer to replace the
/// acceleration's contents from the source before applying anything further
/// (see [`crate::cdc::ChangeEnvelope::history_unavailable`]). The committer
/// rides that same envelope rather than following it, for two reasons: the head
/// must not be persisted until the replacement is durably applied, and a real
/// committer keeps the envelope out of the consumer's zero-row heartbeat
/// stripping — which a no-op one would not survive, leaving the rebuild
/// unrequested.
fn snapshot_boundary_envelope(
    source: &Arc<SharedSource>,
    key: &MemberKey,
    schema: &SchemaRef,
    dataset: String,
    history_unavailable: bool,
) -> Result<ChangeEnvelope> {
    let schema_mismatch = |e: crate::cdc::ChangeBatchError| Error::SchemaMismatch {
        message: e.to_string(),
    };
    let (_, batch, _, _) = crate::cdc::build_heartbeat_envelope(schema, None, false)
        .map_err(schema_mismatch)?
        .into_parts()
        .map_err(schema_mismatch)?;
    Ok(ChangeEnvelope::from_parts(
        Box::new(SnapshotBoundaryCommitter {
            source: Arc::clone(source),
            key: key.clone(),
            dataset,
        }),
        batch,
        // Readiness stays lag-based: a member still loading is not ready.
        false,
        history_unavailable,
    ))
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
    /// The cursor type this member persists (`Gtid` when the source is
    /// GTID-positioning, else `File`), decided at attach. Written into every
    /// checkpoint so a resume reloads the correct positioning mode.
    cursor_type: CursorType,
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
                    connection = %self.key.label(),
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
                    connection = %self.key.label(),
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
    let connection = key.label();

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
    Err(Error::SharedSourceUnavailable { connection })
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
    let connection = source.key.label();

    // No connection-param agreement check is needed: everything that must match
    // for two datasets to safely share one dump — host, port, user, password,
    // TLS config, and `server_id` — is part of the `SourceKey`. A dataset that
    // differs in any of them produces a different key and gets its own dump, so
    // it can never ride a slot-mate's transport, credential, or replica id.

    if let Some(existing) = source.member(&member_key) {
        if existing.sender.is_closed() {
            source.detach_member(&member_key, "superseded by a new subscription", false);
        } else {
            return Err(Error::SharedTableAlreadySubscribed {
                database,
                table,
                connection,
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
    // GTID auto-positioning is used whenever the source reports gtid_mode = ON
    // (all members share the connection, so all agree). Failover-safe; decides
    // the member's cursor type at bootstrap and drives the shared dump.
    let gtid_mode = super::setup::detect_gtid_mode(&mut conn).await?;
    let use_gtid = super::setup::gtid_mode_is_on(gtid_mode.as_deref());
    let cursor_type = if use_gtid {
        CursorType::Gtid
    } else {
        CursorType::File
    };
    // Surface the positioning mode and — when GTID is unavailable — WHY, plus
    // the operational consequence. GTID is auto-on whenever the source reports
    // `gtid_mode = ON`; anything else (OFF, ON_PERMISSIVE, or unreadable) falls
    // back to file+offset, which cannot survive a source failover/promotion.
    if use_gtid {
        tracing::info!(
            dataset = %dataset_name,
            "MySQL replication: GTID auto-positioning active."
        );
    } else {
        tracing::warn!(
            dataset = %dataset_name,
            "MySQL replication: file+offset positioning (gtid_mode is `{}`, not `ON`); resume is not failover-safe - a source failover forces a full re-snapshot.",
            gtid_mode.as_deref().unwrap_or("unavailable")
        );
    }
    let rejoining = lock(&source.detached).remove(&member_key);
    let (floor, gtid_seed, start): (BinlogPosition, GtidSet, MemberStart) = resolve_start_position(
        &mut conn,
        &params,
        &position_store,
        schema_json.as_deref(),
        checkpoint_schema_json.as_deref(),
        &layout_fingerprint,
        &dataset_name,
        &database,
        &table,
        use_gtid,
    )
    .await?;
    if let Err(e) = conn.disconnect().await {
        tracing::debug!(dataset = %dataset_name, error = %e, "setup disconnect");
    }

    // Both loading starts hold the member SNAPSHOTTING until its boundary
    // envelope lands, so neither can advance the shared floor on rows the
    // consumer has not applied yet.
    let snapshotting = start.is_loading();
    let (sender, receiver) = mpsc::channel(DEFAULT_MEMBER_CHANNEL_CAPACITY);
    source
        .ack
        .register_with_gtid(&member_key, floor, gtid_seed, snapshotting);
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
            cursor_type,
        }),
    );
    metrics.mark_member_attached();

    tracing::info!(
        dataset = %dataset_name,
        connection = %source.key.label(),
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
    // promotes it and reconnects), the consumer-side rebuild that replaces it
    // when the acceleration already holds rows, or an immediate empty head on
    // resume.
    let head: ChangesStream = match start {
        MemberStart::Rebuild => {
            // One zero-row envelope asking the consumer to replace the
            // acceleration's contents from the source, and nothing else: the
            // replacement is atomic, so queries keep seeing the pre-rebuild
            // contents until it swaps. Clearing the table here and letting the
            // snapshot refill it — what a `truncate` prelude does — is
            // observable to queries as an empty, then partially filled, table.
            let signal = snapshot_boundary_envelope(
                source,
                &member_key,
                &schema,
                dataset_name.clone(),
                true,
            )?;
            tracing::warn!(
                dataset = %dataset_name,
                connection = %source.key.label(),
                "the persisted binlog position for this dataset is unusable and its acceleration survives restarts, so it will be rebuilt from the source before changes are applied"
            );
            // No snapshot runs on this path — the consumer's rebuild replaces it
            // — so the gauge's "finished, or skipped" state is reached here.
            // Leaving it at 0 would strand every readiness probe reading it.
            metrics.mark_bootstrap_complete();
            Box::pin(stream::once(async move { Ok(signal) }))
        }
        MemberStart::Snapshot => {
            // Clear whatever a crashed earlier load left behind, so the snapshot
            // is a full replace rather than an upsert over rows the source no
            // longer has. Reached only when nothing was ever persisted for this
            // member, so there is no completed load to preserve; an acceleration
            // that has one takes the `Rebuild` arm above.
            let truncate = super::truncate_envelope(&schema, &primary_keys, &column_map)?;
            // Built before the snapshot input takes ownership of `dataset_name`.
            //
            // Snapshot completion is signalled by a real boundary committer, NOT
            // a stream-drain hook: the committer's `commit()` runs on the apply
            // loop only after every prior (snapshot) envelope is durably applied,
            // whereas a drain hook fires when the reader pulls it — up to a
            // prefetch-channel ahead of durable apply. Promoting + persisting
            // this member's head before its snapshot is durably applied would
            // lose base rows on a crash. On a snapshot error the stream ends
            // before this envelope, so the member stays SNAPSHOTTING and
            // re-snapshots on rejoin (see `detach_member`). `persist_all` also
            // skips SNAPSHOTTING members until this fires.
            let boundary = snapshot_boundary_envelope(
                source,
                &member_key,
                &schema,
                dataset_name.clone(),
                false,
            )?;
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
            Box::pin(
                stream::once(async move { Ok(truncate) })
                    .chain(snapshot)
                    .chain(stream::once(async move { Ok(boundary) })),
            )
        }
        MemberStart::Stream => {
            metrics.mark_bootstrap_complete();
            Box::pin(stream::empty::<
                std::result::Result<ChangeEnvelope, StreamError>,
            >())
        }
    };

    Ok(Box::pin(head.chain(ReceiverStream::new(receiver))))
}

/// Whether a persisted checkpoint can be restored against the live source, or
/// must be discarded because the source no longer contains it (a reset,
/// purge, or a different/rebuilt server). Pure so the reset-detection decision
/// is unit-testable without a live `MySQL` — the caller supplies the live
/// source state it fetched.
#[derive(Debug, PartialEq, Eq)]
enum CheckpointVerdict {
    /// The checkpoint is consistent with the current source; resume from it.
    Resume,
    /// The source no longer contains the checkpoint; apply
    /// `invalid_checkpoint_behavior`. Carries the operator-facing reason.
    Unresumable(&'static str),
}

/// GTID verdict: the persisted executed set must be a subset of the source's
/// current `@@gtid_executed`. A `RESET MASTER`, a rebuilt server (fresh
/// `server_uuid`), or a different source reports a set that no longer contains
/// the checkpoint — resuming would position `COM_BINLOG_DUMP_GTID` from a set
/// the server cannot honor and silently serve pre-reset data.
fn gtid_checkpoint_verdict(persisted: &GtidSet, source_executed: &GtidSet) -> CheckpointVerdict {
    if persisted.is_subset_of(source_executed) {
        CheckpointVerdict::Resume
    } else {
        CheckpointVerdict::Unresumable(
            "the source's GTID history diverged from the checkpoint (RESET MASTER, a rebuilt server, or a different source); its executed set no longer contains the persisted position",
        )
    }
}

/// File+offset verdict: the persisted binlog file must still be present in the
/// source's binary log index. A purge, or a reset whose binlog numbering
/// restarted below the checkpoint's file, drops it.
fn file_checkpoint_verdict(persisted_file_present: bool) -> CheckpointVerdict {
    if persisted_file_present {
        CheckpointVerdict::Resume
    } else {
        CheckpointVerdict::Unresumable(
            "the persisted binlog file is no longer present on the source (purged, or a reset restarted binlog numbering)",
        )
    }
}

/// How a member's stream starts once its position has been resolved.
///
/// The distinction that matters is between the two loading starts: a snapshot
/// refills an acceleration that holds nothing worth keeping, while a rebuild
/// replaces one that is already serving rows and must never be emptied first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberStart {
    /// Resume from the persisted position; nothing to load.
    Stream,
    /// Snapshot into an acceleration with no completed load behind it: no
    /// position was ever persisted for this member.
    Snapshot,
    /// A position was persisted but cannot be resumed from, so the acceleration
    /// holds rows whose explanation is gone from the source's binary log. Only
    /// re-reading the table can correct them, and the consumer owns the
    /// accelerator so it can do that as one atomic replacement — see
    /// [`crate::cdc::ChangeEnvelope::history_unavailable`].
    Rebuild,
}

impl MemberStart {
    /// Whether the member is loading its acceleration, by either route. Held
    /// `SNAPSHOTTING` in the ack until its boundary envelope lands.
    const fn is_loading(self) -> bool {
        matches!(self, Self::Snapshot | Self::Rebuild)
    }
}

/// Resolve a fresh (non-rejoin) member's start position, applying
/// `invalid_checkpoint_behavior` per member. Returns
/// `(floor, gtid_seed, start)`. `gtid_seed` is the executed GTID set to
/// seed the member's ack from (empty when the dump is file-positioning);
/// `use_gtid` is the source's current mode (all members share it).
#[expect(
    clippy::too_many_arguments,
    reason = "resolve threads the member's identity, checkpoint meta, and source \
              mode; bundling into a struct would only relocate the argument list"
)]
async fn resolve_start_position(
    conn: &mut Conn,
    params: &ReplicationParams,
    position_store: &Arc<dyn PositionStore>,
    schema_json: Option<&str>,
    checkpoint_schema_json: Option<&str>,
    layout_fingerprint: &str,
    dataset_name: &str,
    database: &str,
    table: &str,
    use_gtid: bool,
) -> Result<(BinlogPosition, GtidSet, MemberStart)> {
    let persisted = position_store
        .load()
        .await
        .map_err(|e| Error::PositionStoreAccess {
            message: e.to_string(),
        })?;

    // A recorded position proves the acceleration is holding rows: the store is
    // durable (an accelerator that does not survive restarts is wired to
    // `NoopPositionStore`, which records nothing) and a load once ran far enough
    // to record progress. Such an acceleration must be replaced, not emptied and
    // refilled.
    //
    // The converse does not hold, and this deliberately under-claims rather than
    // guessing: a durable acceleration whose sidecar failed to open also records
    // nothing, and reads here as a first load. Closing that needs the store to
    // say whether it can record at all, and the params to say whether the
    // acceleration survives restarts — the shape `postgres_replication` uses.
    // Tracked in #13021.
    let has_recorded_position = persisted.is_some();

    // `Some((position, gtid_seed))` to resume, `None` to (re)snapshot.
    let resume: Option<(BinlogPosition, GtidSet)> = match persisted {
        Some(_) if params.snapshot_mode == InitialSnapshotMode::Always => None,
        Some(persisted) => {
            if check_resume_compatibility(
                persisted.schema_json.as_deref(),
                schema_json,
                layout_fingerprint,
            )
            .is_err()
            {
                apply_invalid_checkpoint(params, dataset_name, "layout/schema drift")?;
                None
            } else {
                match persisted.cursor_type {
                    // GTID resume: the server computes the start point from the
                    // executed set, so no binlog-file pre-check. A checkpoint
                    // bootstrapped by GTID cannot silently downgrade to
                    // file+offset (that would resume from a server-local offset
                    // unrelated to the applied set) — if the source no longer
                    // reports `gtid_mode = ON`, that is a hard error.
                    CursorType::Gtid => {
                        if !use_gtid {
                            return Err(Error::GtidResumeUnavailable {
                                dataset: dataset_name.to_string(),
                                database: database.to_string(),
                                table: table.to_string(),
                            });
                        }
                        // A stored *empty* set is legitimate (`Some("")` →
                        // empty set: `gtid_mode = ON` with zero txns applied
                        // yet). A *missing* set (`None`) or an unparseable one
                        // is a corrupt/incomplete checkpoint — not a
                        // known-empty set — so it must not silently resume from
                        // the start of the source's binlogs; it honors
                        // `invalid_position_behavior`.
                        let parsed = match persisted.gtid_set.as_deref() {
                            Some(raw) => {
                                GtidSet::parse(raw).map_err(|_| "corrupt persisted GTID set")
                            }
                            None => Err("GTID checkpoint has no executed set \
                                         (corrupt or incomplete)"),
                        };
                        match parsed {
                            Ok(set) => {
                                // Validate the checkpoint is still real on THIS source:
                                // its executed set must be a subset of the source's current `@@gtid_executed`.
                                let source_executed =
                                    super::setup::fetch_executed_gtid_set(conn).await?;
                                match gtid_checkpoint_verdict(&set, &source_executed) {
                                    CheckpointVerdict::Resume => Some((persisted.position, set)),
                                    CheckpointVerdict::Unresumable(reason) => {
                                        apply_invalid_checkpoint(params, dataset_name, reason)?;
                                        None
                                    }
                                }
                            }
                            Err(reason) => {
                                apply_invalid_checkpoint(params, dataset_name, reason)?;
                                None
                            }
                        }
                    }
                    CursorType::File => {
                        let present =
                            super::setup::binlog_file_exists(conn, &persisted.position.file)
                                .await?;
                        match file_checkpoint_verdict(present) {
                            CheckpointVerdict::Resume => Some((persisted.position, GtidSet::new())),
                            CheckpointVerdict::Unresumable(reason) => {
                                apply_invalid_checkpoint(params, dataset_name, reason)?;
                                None
                            }
                        }
                    }
                }
            }
        }
        None => None,
    };

    if let Some((position, gtid_seed)) = resume {
        // Report the cursor actually used. In GTID mode the pump ignores
        // file+offset and positions purely from the executed set
        // (`start_binlog_stream`), so logging `position=file:offset` there is
        // misleading — surface the GTID set instead. File+offset positioning
        // logs the file:offset it truly resumes from.
        if use_gtid {
            tracing::info!(dataset = %dataset_name, gtid_set = %gtid_seed, "shared mysql binlog: resuming from persisted GTID position");
        } else {
            tracing::info!(dataset = %dataset_name, position = %position, "shared mysql binlog: resuming from persisted file+offset position");
        }
        return Ok((position, gtid_seed, MemberStart::Stream));
    }

    // No usable position: capture the head (and its executed GTID set, when
    // GTID-positioning) first so the snapshot overlap replays idempotently, then
    // either snapshot from it or (snapshot disabled) stream from it after
    // persisting it up front.
    let (head, head_gtid) = if use_gtid {
        super::setup::fetch_head_and_gtid(conn).await?
    } else {
        (
            super::setup::fetch_head_position(conn).await?,
            GtidSet::new(),
        )
    };
    if params.snapshot_mode == InitialSnapshotMode::Disabled {
        let initial = PersistedPosition {
            position: head.clone(),
            schema_json: checkpoint_schema_json.map(ToString::to_string),
            gtid_set: use_gtid.then(|| head_gtid.to_string()),
            cursor_type: if use_gtid {
                CursorType::Gtid
            } else {
                CursorType::File
            },
        };
        if let Err(e) = position_store.save(&initial).await {
            tracing::warn!(dataset = %dataset_name, error = %e, "failed to persist initial binlog head");
        }
        Ok((head, head_gtid, MemberStart::Stream))
    } else if has_recorded_position {
        Ok((head, head_gtid, MemberStart::Rebuild))
    } else {
        Ok((head, head_gtid, MemberStart::Snapshot))
    }
}

/// Apply `invalid_checkpoint_behavior` for one member: `Error` fails the
/// member's stream; `Restart` clears its saved position so it re-snapshots.
fn apply_invalid_checkpoint(
    params: &ReplicationParams,
    dataset_name: &str,
    reason: &str,
) -> Result<()> {
    match params.invalid_position_behavior {
        InvalidCheckpointBehavior::Error => super::StalePositionSnafu {
            // `reason` describes the actual condition (layout/schema drift, a
            // purged binlog file, or a GTID-history divergence/reset) — surface
            // it verbatim rather than a fixed explanation, since this helper now
            // covers all three.
            message: format!(
                "cannot resume mysql binlog for {dataset_name}: {reason}. Resuming could serve \
                 incorrect data. Set `mysql_replication_invalid_checkpoint_behavior: restart` to \
                 rebuild the acceleration from the source instead. See: \
                 https://spiceai.org/docs/components/data-connectors/mysql"
            ),
        }
        .fail(),
        InvalidCheckpointBehavior::Restart => {
            tracing::warn!(dataset = %dataset_name, reason, "persisted mysql binlog checkpoint unusable; rebuilding");
            // Deliberately left in place until the rebuild's boundary committer
            // replaces it with the new head — same invariant, and same reasoning,
            // as `rebootstrap_member`.
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
    tracing::error!(connection = %source.key.label(), "shared mysql binlog stream failed: {message}");
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

/// Whether the source uses GTID auto-positioning (`gtid_mode = ON`) for this
/// shared dump — decided once at the source level (all members share the
/// connection). Best-effort: any probe failure falls back to file+offset
/// positioning.
async fn detect_source_gtid(params: &ReplicationParams) -> bool {
    match super::setup::connect(params).await {
        Ok(mut conn) => {
            let mode = super::setup::detect_gtid_mode(&mut conn)
                .await
                .ok()
                .flatten();
            let on = super::setup::gtid_mode_is_on(mode.as_deref());
            if let Err(e) = conn.disconnect().await {
                tracing::debug!(error = %e, "shared mysql gtid-detect disconnect");
            }
            on
        }
        Err(e) => {
            tracing::debug!(error = %e, "shared mysql gtid-mode detection failed; using file+offset");
            false
        }
    }
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
    let connection = source.key.label();
    let mut backoff = super::resilience::StreamBackoff::default_for_stream();
    let mut reconnect_attempts: u32 = 0;
    let idle_tick = crate::cdc::heartbeat_interval(params.ready_lag)
        .min(params.checkpoint_interval)
        .min(IDLE_TICK_CAP);
    let mut side_conn: Option<Conn> = None;
    // Positioning mode for this shared dump, decided once at the source level
    // (all members share the connection, so all see the same `gtid_mode`): GTID
    // auto-positioning when the source reports `gtid_mode = ON`, else
    // file+offset. When on, the dump is opened with `COM_BINLOG_DUMP_GTID` from
    // the intersection of members' executed sets, giving failover-safe resume.
    let use_gtid = detect_source_gtid(&params).await;
    let mut last_persist_at = Instant::now();
    // Throttles the head-poll + readiness heartbeat. Emitted on BOTH the idle
    // branch and the event path: the server streams its own binlog heartbeats
    // every ~checkpoint_interval/2, so on a caught-up source `stream.next()`
    // resolves before the idle timeout and the idle branch is starved — without
    // an event-path emission a quiet-but-heartbeating source would never reach
    // Ready (the per-dataset stream emits readiness on both paths too).
    let mut last_heartbeat_at = Instant::now();
    // Last checkpoint identity persisted per member, to skip no-op sidecar
    // writes (position + GTID set — see `PersistIdentity`).
    let mut last_persisted: HashMap<MemberKey, PersistIdentity> = HashMap::new();

    'reconnect: loop {
        if crate::cdc::shutdown_epoch() != shutdown_epoch {
            persist_all(&source, &mut last_persisted).await;
            tracing::info!(connection = %connection, "runtime shutdown; releasing shared mysql binlog connection");
            finish_pump(&source);
            return;
        }
        source.reap_closed_members();
        if source.live_member_count() == 0 && try_finish_if_empty(&source).await {
            tracing::info!(connection = %connection, "all members detached; shutting down shared mysql binlog stream");
            return;
        }
        source.restart_requested.store(false, Ordering::Release);

        let Some(resume) = source.ack.flush_position() else {
            // No members yet (raced a detach); loop to re-check / finish.
            continue 'reconnect;
        };
        // GTID resume set: the intersection across members — the GTIDs every
        // member has applied, so the dump re-sends anything any member still
        // needs (ahead members suppress via their committed floor). Empty (⇒
        // file+offset) when not GTID-positioning.
        let resume_gtid = if use_gtid {
            source.ack.resume_gtid().unwrap_or_default()
        } else {
            GtidSet::new()
        };

        let mut stream = match open_binlog_stream(
            &params,
            &resume,
            &connection,
            use_gtid,
            &resume_gtid,
        )
        .await
        {
            Ok(stream) => {
                backoff.reset();
                if reconnect_attempts > 0 {
                    // In GTID mode the dump repositions from the shared executed
                    // set, not `resume` (the file+offset floor) — report the set.
                    if use_gtid {
                        tracing::info!(connection = %connection, attempts = reconnect_attempts, gtid_set = %resume_gtid, "shared mysql binlog connection resumed");
                    } else {
                        tracing::info!(connection = %connection, attempts = reconnect_attempts, position = %resume, "shared mysql binlog connection resumed");
                    }
                    reconnect_attempts = 0;
                }
                // Connection starts at the shared min (<= every held member's
                // floor), so every snapshot-complete member's gap is covered:
                // promote them to routable + creditable.
                source.ack.promote_ready_members();
                stream
            }
            Err(e) if super::resilience::is_purged_position_error(&e) => {
                // The shared min was purged from the source. Honor
                // `invalid_position_behavior`: re-snapshot in place (restart) or
                // broadcast the fatal purge error (error).
                match handle_purged_position(&source, &params, use_gtid, &resume, &connection).await
                {
                    PurgeOutcome::Rebootstrapped => continue 'reconnect,
                    PurgeOutcome::Fatal => break 'reconnect,
                }
            }
            Err(e) if super::resilience::is_transient_mysql(&e) => {
                for (_, m) in source.live_members() {
                    m.metrics.inc_reconnect();
                }
                reconnect_attempts = reconnect_attempts.saturating_add(1);
                log_transient_reconnect(
                    reconnect_attempts,
                    &connection,
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
        // GTID of the transaction currently being read (captured at its
        // `GtidEvent`, folded into members' executed sets at commit/idle-credit).
        // `None` between transactions or when not GTID-positioning.
        let mut current_txn_gtid: Option<(uuid::Uuid, u64)> = None;

        'recv: loop {
            if crate::cdc::shutdown_epoch() != shutdown_epoch {
                if let Err(e) = stream.close().await {
                    tracing::debug!(connection = %connection, error = %e, "binlog close during shutdown");
                }
                persist_all(&source, &mut last_persisted).await;
                tracing::info!(connection = %connection, "runtime shutdown; released shared mysql binlog connection");
                finish_pump(&source);
                return;
            }
            if source.restart_requested.swap(false, Ordering::AcqRel) {
                tracing::debug!(connection = %connection, "reconnecting shared mysql binlog stream to pick up membership change");
                break 'recv;
            }
            source.reap_closed_members();
            if source.live_member_count() == 0 && try_finish_if_empty(&source).await {
                tracing::info!(connection = %connection, "all members detached; shutting down shared mysql binlog stream");
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
                    last_heartbeat_at = Instant::now();
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
                    &connection,
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
                    match handle_purged_position(&source, &params, use_gtid, &resume, &connection)
                        .await
                    {
                        PurgeOutcome::Rebootstrapped => continue 'reconnect,
                        PurgeOutcome::Fatal => break 'reconnect,
                    }
                }
                Err(e) if super::resilience::is_transient_mysql(&e) => {
                    for (_, m) in source.live_members() {
                        m.metrics.inc_recv_error();
                        m.metrics.inc_reconnect();
                    }
                    reconnect_attempts = reconnect_attempts.saturating_add(1);
                    log_transient_reconnect(
                        reconnect_attempts,
                        &connection,
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
            // Offset within `current_file` this event advances the stream to. A
            // real `ROTATE` rewrites both together (see [`rotate_target`]).
            let mut event_end_pos = u64::from(header.log_pos());
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
                    // Take BOTH the file and the offset from the event: its
                    // header offset belongs to the file being closed, so keeping
                    // `event_end_pos` here would credit idle members a position
                    // the newly opened file will not reach for a long time. See
                    // [`rotate_target`].
                    if let Some(target) = rotate_target(&rotate) {
                        current_file = target.file;
                        event_end_pos = target.pos;
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
                    // Every decode is routed from here, so this is the one place
                    // that can check the layout against the event describing the
                    // row images it will decode. A layout adopted from
                    // `information_schema` reflects the source *now*, which under
                    // lag can be a later DDL than the events in flight (#11764);
                    // fail this member closed rather than scramble its columns.
                    if let Some(mismatch) = layout_event_mismatch(&layout.layout, &tme) {
                        member.metrics.inc_schema_mismatch_error();
                        routes.remove(&table_id);
                        member_fatal(&source, &mkey, format!(
                            "source table {}.{} does not match the shape of the changes being replicated: column {} (position {}) is `{}` in the table definition but the change events carry a different type there. This happens when the source applies more than one ALTER TABLE while replication is behind, so the table definition read for the first one already reflects the second. Let replication catch up before the next schema change, then re-bootstrap by setting `mysql_replication_invalid_checkpoint_behavior: restart`.",
                            mkey.0, mkey.1, mismatch.column, mismatch.ordinal, mismatch.source_type
                        )).await;
                        continue 'recv;
                    }
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
                        current_txn_gtid,
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
                                current_txn_gtid,
                            )
                            .await;
                        }
                        QueryKind::Xa => {
                            tracing::warn!(connection = %connection, statement = %statement, "XA transaction observed on the shared binlog; XA transactions are not supported and their changes are ignored");
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
                                current_txn_gtid,
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
                Some(EventData::GtidEvent(gtid_event)) => {
                    // A GTID event opens a transaction group ahead of its
                    // BEGIN/statement. Capture the GTID so the commit (and the
                    // idle-credit of members with no change) folds it into their
                    // executed sets for failover-safe resume.
                    txn_open = true;
                    current_txn_gtid =
                        Some((uuid::Uuid::from_bytes(gtid_event.sid()), gtid_event.gno()));
                }
                Some(EventData::AnonymousGtidEvent(_)) => {
                    // An anonymous transaction carries no GTID. Under GTID
                    // positioning it must not happen (source not fully
                    // `gtid_mode = ON`): the executed set could not describe it,
                    // so fail the whole group loudly rather than persist a set
                    // that silently omits transactions. Each member re-evaluates
                    // its own persisted checkpoint on the next (re)subscribe.
                    if use_gtid {
                        fatal_broadcast(
                            &source,
                            Error::AnonymousTransactionUnderGtid {
                                dataset: connection.clone(),
                            }
                            .to_string(),
                        )
                        .await;
                        break 'reconnect;
                    }
                    txn_open = true;
                    current_txn_gtid = None;
                }
                Some(_) | None => {}
            }

            // Idle safe-advance: with no open transaction, everything up to this
            // event's end is either applied or irrelevant to every member. Credit
            // idle streaming members so a group with quiet members still advances
            // its resume past foreign-table traffic.
            if !txn_open && event_end_pos >= MIN_VALID_EVENT_POS {
                let pos = BinlogPosition::new(current_file.clone(), event_end_pos);
                source.ack.credit_idle(&pos, current_txn_gtid);
            }

            if last_persist_at.elapsed() >= params.checkpoint_interval {
                persist_all(&source, &mut last_persisted).await;
                last_persist_at = Instant::now();
            }

            // Event-path readiness: the server's own binlog heartbeats keep this
            // loop from ever hitting the idle branch on a caught-up source, so
            // publish head/lag metrics and fan a readiness heartbeat here too,
            // throttled to the idle-tick cadence.
            if last_heartbeat_at.elapsed() >= idle_tick {
                poll_head_and_heartbeat(&source, &mut side_conn, &params).await;
                last_heartbeat_at = Instant::now();
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

/// Where a `ROTATE` event repositions the shared stream, or `None` for the fake
/// rotate the server sends at the head of a dump (which opens no new file).
///
/// `ROTATE` is the one event whose header offset does **not** belong to the file
/// it names: `log_pos` is the end of the event in the file being *closed*, while
/// the payload names the file being *opened* and the offset to resume reading at
/// (normally 4, just past the magic number). Pairing the new name with the
/// closing file's offset yields a coordinate far beyond anything the new file
/// holds — an idle member credited there has every later commit in that file
/// suppressed by [`AckSlot::already_committed`], silently and for as long as the
/// new file stays smaller than the old one (#12042).
fn rotate_target(rotate: &RotateEvent<'_>) -> Option<BinlogPosition> {
    (!rotate.is_fake()).then(|| BinlogPosition::new(rotate.name(), rotate.position()))
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
    gtid: Option<(uuid::Uuid, u64)>,
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
                gtids: gtid.into_iter().collect(),
            }),
            Box::new(rows),
            is_ready,
        );
        slot.deliver(commit_pos);
        match deliver_to_member(
            &member.metrics,
            &member.sender,
            Ok(envelope),
            shutdown_epoch,
            &member.dataset_name,
        )
        .await
        {
            DeliverOutcome::Sent => {}
            DeliverOutcome::ReceiverGone => {
                source.detach_member(key, "changes stream receiver dropped", true);
            }
            DeliverOutcome::ShutdownAbandon => return,
        }
    }
    source.ack.credit_idle(commit_pos, gtid);
}

/// Route a statement affecting a subscribed table: TRUNCATE is applied as a
/// change; a schema-change DDL is member-fatal (adoption happens on the next
/// `TableMap` for compatible changes).
#[expect(
    clippy::too_many_arguments,
    reason = "each arg is a distinct piece of binlog-event context"
)]
async fn handle_statement(
    source: &Arc<SharedSource>,
    statement: &str,
    default_db: &str,
    current_file: &str,
    event_end_pos: u64,
    event_timestamp: u32,
    shutdown_epoch: u64,
    gtid: Option<(uuid::Uuid, u64)>,
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
                        gtids: gtid.into_iter().collect(),
                    }),
                    batch,
                    is_ready,
                );
                slot.deliver(&commit_pos);
                match deliver_to_member(
                    &member.metrics,
                    &member.sender,
                    Ok(envelope),
                    shutdown_epoch,
                    &member.dataset_name,
                )
                .await
                {
                    DeliverOutcome::Sent => {}
                    DeliverOutcome::ReceiverGone => {
                        source.detach_member(&mkey, "changes stream receiver dropped", true);
                    }
                    DeliverOutcome::ShutdownAbandon => return,
                }
            }
            StatementKind::SchemaChange("ALTER TABLE") => {
                // A compatible ALTER (e.g. ADD COLUMN) is adopted mid-stream by
                // re-fetching the source layout and reconciling it against the
                // dataset schema — the same behavior as the per-dataset path.
                // Columns the source gained but the dataset does not declare are
                // ignored; an ALTER that drops or retypes a *dataset* column
                // cannot be adopted and is member-fatal. Adopting on the
                // statement (not only on the next TableMap's column-count change)
                // also catches type-only ALTERs that keep the column count.
                let old_layout = { lock(&member.layout).layout.clone() };
                match adopt_current_layout(
                    &source.params,
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
                    }) => {
                        *lock(&member.layout) = Arc::new(MemberLayout {
                            layout,
                            column_map,
                            pk_source_indexes,
                        });
                    }
                    Err(e) => {
                        member.metrics.inc_schema_mismatch_error();
                        member_fatal(source, &mkey, format!(
                            "ALTER TABLE on source table {}.{} (statement: {statement}) cannot be adopted mid-stream: {e}. Update the dataset schema to match the new table definition, or re-bootstrap by setting `mysql_replication_invalid_checkpoint_behavior: restart`.",
                            mkey.0, mkey.1
                        )).await;
                    }
                }
            }
            StatementKind::SchemaChange(verb) => {
                // DROP / RENAME / DROP DATABASE: the subscribed table no longer
                // exists under this name — member-fatal. Only the affected member
                // is torn down; the rest of the shared dump keeps running.
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
    metrics: &MetricsCollector,
    sender: &mpsc::Sender<std::result::Result<ChangeEnvelope, StreamError>>,
    envelope: std::result::Result<ChangeEnvelope, StreamError>,
    shutdown_epoch: u64,
    dataset: &str,
) -> DeliverOutcome {
    let mut pending = envelope;
    let mut stalled_for = Duration::ZERO;
    loop {
        match sender.send_timeout(pending, MEMBER_SEND_STALL_WARN).await {
            Ok(()) => return DeliverOutcome::Sent,
            Err(mpsc::error::SendTimeoutError::Closed(_)) => return DeliverOutcome::ReceiverGone,
            Err(mpsc::error::SendTimeoutError::Timeout(returned)) => {
                if crate::cdc::shutdown_epoch() != shutdown_epoch {
                    return DeliverOutcome::ShutdownAbandon;
                }
                stalled_for += MEMBER_SEND_STALL_WARN;
                // The pump reads the dump socket for the whole group, so this
                // wait is also time the socket goes undrained: past the
                // session's `net_write_timeout` the source aborts the dump and
                // every member on it resumes from its acked position. Counted,
                // not only logged, so a wedged apply stays visible after the
                // warning scrolls away.
                metrics.add_send_stalled(MEMBER_SEND_STALL_WARN.as_secs());
                tracing::warn!(dataset = %dataset, stalled_for = ?stalled_for, "shared mysql binlog member sink is not draining; the pump is waiting to deliver committed changes (watch dataset_mysql_replication_member_send_stalled_seconds_total)");
                pending = returned;
            }
        }
    }
}

/// Build a member's [`PersistedPosition`] from its ack slot — capturing the
/// executed GTID set + cursor type when GTID-positioning (so a resume reloads
/// the same mode), else a plain file+offset checkpoint. Shared by `persist_all`
/// and the snapshot-boundary committer so both write identical checkpoint shape.
fn persisted_for(member: &MemberHandle, slot: &AckSlot) -> PersistedPosition {
    PersistedPosition {
        position: slot.committed(),
        schema_json: member.checkpoint_schema_json.clone(),
        gtid_set: (member.cursor_type == CursorType::Gtid)
            .then(|| slot.gtid_snapshot().to_string()),
        cursor_type: member.cursor_type,
    }
}

/// The checkpoint identity used to skip no-op sidecar writes: the committed
/// position AND the executed GTID set. Both must be compared — after a source
/// failover a member's file+offset can freeze (the promoted primary streams
/// from a lower ordinal, which monotonic-max rejects) while its GTID set keeps
/// growing; deduping on position alone would then stop persisting and let the
/// crash-replay window grow without bound.
type PersistIdentity = (BinlogPosition, Option<String>);

/// Outcome of handling a purged shared-resume position: the pump either
/// re-snapshotted its members in place and should reconnect, or gave up and
/// broadcast a fatal error.
enum PurgeOutcome {
    Rebootstrapped,
    Fatal,
}

/// The shared resume position was purged from the source. Honor
/// `invalid_position_behavior`:
///   - `Error` (default): broadcast the fatal purge error — the dataset stays
///     errored until the operator intervenes (widen `binlog_expire_logs_seconds`
///     or switch to `restart`).
///   - `Restart`: re-snapshot every live member in place from the current head.
///     The runtime holds one long-lived `ChangesStream` and never re-subscribes,
///     so recovery is delivered THROUGH each member's live channel rather than
///     by re-running `resolve_start_position` — see [`rebootstrap_member`]. This
///     is what makes `restart` actually recover a purged position instead of
///     being a no-op that only takes effect on a full process restart.
async fn handle_purged_position(
    source: &Arc<SharedSource>,
    params: &ReplicationParams,
    use_gtid: bool,
    resume: &BinlogPosition,
    connection: &str,
) -> PurgeOutcome {
    match params.invalid_position_behavior {
        InvalidCheckpointBehavior::Restart => {
            tracing::warn!(
                connection = %connection,
                purged_position = %resume,
                "shared mysql binlog resume position was purged; invalid_checkpoint_behavior=restart, re-snapshotting all members from the current source head"
            );
            match rebootstrap_all_for_restart(source, params, use_gtid).await {
                Ok(()) => PurgeOutcome::Rebootstrapped,
                Err(e) => {
                    fatal_broadcast(
                        source,
                        format!("mysql binlog re-snapshot after a purged position failed: {e}"),
                    )
                    .await;
                    PurgeOutcome::Fatal
                }
            }
        }
        InvalidCheckpointBehavior::Error => {
            fatal_broadcast(
                source,
                purged_position_error(resume, connection).to_string(),
            )
            .await;
            PurgeOutcome::Fatal
        }
    }
}

/// Re-snapshot every live member from a single freshly-captured head. Capturing
/// the head once keeps all members aligned on one valid resume point, so the
/// next `open_binlog_stream` opens from it (the purged position is discarded).
async fn rebootstrap_all_for_restart(
    source: &Arc<SharedSource>,
    params: &ReplicationParams,
    use_gtid: bool,
) -> Result<()> {
    let mut conn = super::setup::connect(params).await?;
    let (head, head_gtid) = if use_gtid {
        super::setup::fetch_head_and_gtid(&mut conn).await?
    } else {
        (
            super::setup::fetch_head_position(&mut conn).await?,
            GtidSet::new(),
        )
    };
    if let Err(e) = conn.disconnect().await {
        tracing::debug!(error = %e, "re-snapshot head-fetch disconnect");
    }
    tracing::info!(
        connection = %source.key.label(),
        new_head = %head,
        "re-snapshotting shared mysql binlog members from the current source head"
    );
    for (key, member) in source.live_members() {
        if member.sender.is_closed() {
            continue;
        }
        rebootstrap_member(source, &key, &member, &head, &head_gtid).await?;
    }
    Ok(())
}

/// Rebuild one member in place after its resume position was purged and
/// `invalid_position_behavior = Restart`. Resets the slot to `head` held
/// `SNAPSHOTTING`, then pushes one rebuild signal through the member's LIVE
/// channel — delivered mid-stream because the runtime holds a single
/// `ChangesStream` and never re-subscribes.
///
/// The purged checkpoint is deliberately left in place until the boundary
/// committer replaces it with the new head. It is the only durable evidence that
/// this acceleration holds rows no position explains: clearing it up front and
/// then crashing mid-rebuild would leave a populated acceleration with no
/// checkpoint, which the next start reads as a first load — and a first load
/// empties the table and refills it from a snapshot, which is the very window
/// this exists to close. Leaving it costs at most one repeated rebuild after a
/// crash, since the next start re-detects it as unusable and lands here again.
///
/// The member is serving rows here, so the acceleration is replaced rather than
/// refilled: the consumer answers
/// [`crate::cdc::ChangeEnvelope::history_unavailable`] with one atomic overwrite
/// and queries keep seeing the pre-rebuild contents until it swaps. Clearing the
/// table and streaming a fresh snapshot into it — the shape this replaced —
/// is observable to every query for the length of the re-read as an empty, then
/// partially filled, table.
///
/// The signal's [`SnapshotBoundaryCommitter`] clears `SNAPSHOTTING`, persists the
/// new head, and requests the reconnect that promotes the member back to
/// streaming, all after the consumer has durably applied the replacement.
async fn rebootstrap_member(
    source: &Arc<SharedSource>,
    key: &MemberKey,
    member: &Arc<MemberHandle>,
    head: &BinlogPosition,
    head_gtid: &GtidSet,
) -> Result<()> {
    // Reset to head, held SNAPSHOTTING so `persist_all`/promotion skip it until
    // the boundary lands — identical to a loading member.
    source
        .ack
        .register_with_gtid(key, head.clone(), head_gtid.clone(), true);

    let signal = snapshot_boundary_envelope(
        source,
        key,
        &member.schema,
        member.dataset_name.clone(),
        true,
    )?;
    if member.sender.send(Ok(signal)).await.is_err() {
        source.detach_member(key, "changes stream receiver dropped during rebuild", true);
    }
    Ok(())
}

/// Persist each member's own committed position to its own sidecar (skipping
/// no-op writes). The shared resume is the min across these on restart.
async fn persist_all(source: &Arc<SharedSource>, last: &mut HashMap<MemberKey, PersistIdentity>) {
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
        let persisted = persisted_for(&member, &slot);
        let identity: PersistIdentity = (persisted.position.clone(), persisted.gtid_set.clone());
        if last.get(&key) == Some(&identity) {
            continue;
        }
        match member.position_store.save(&persisted).await {
            Ok(()) => {
                member.metrics.inc_checkpoint_persist();
                let committed = &persisted.position;
                member
                    .metrics
                    .set_committed_position(committed.file_ordinal().unwrap_or(0), committed.pos);
                last.insert(key, identity);
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
                tracing::debug!(connection = %source.key.label(), error = %e, "shared head-poll connect failed");
                return;
            }
        },
    };
    let head = match super::setup::fetch_head_position(conn).await {
        Ok(head) => head,
        Err(e) => {
            tracing::debug!(connection = %source.key.label(), error = %e, "shared head poll failed");
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
            tracing::debug!(connection = %source.key.label(), error = %e, "shared source-clock query failed");
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
    use crate::mysql_replication::metrics::Metrics;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn key(db: &str, t: &str) -> MemberKey {
        (db.to_string(), t.to_string())
    }
    fn pos(file: &str, p: u64) -> BinlogPosition {
        BinlogPosition::new(file, p)
    }

    const SRC_A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const SRC_B: &str = "5d1c0d8c-71ca-11e1-9e33-c80aa9429999";

    fn gtids(raw: &str) -> GtidSet {
        GtidSet::parse(raw).expect("parse gtid set")
    }

    fn test_params() -> ReplicationParams {
        ReplicationParams {
            opts: mysql_async::Opts::from_url("mysql://user:pass@localhost:3306/db")
                .expect("parse test connection url"),
            server_id: 1,
            snapshot_mode: InitialSnapshotMode::default(),
            bootstrap_batch_size: 1,
            checkpoint_interval: Duration::from_secs(1),
            invalid_position_behavior: InvalidCheckpointBehavior::Restart,
            ready_lag: Duration::from_secs(2),
        }
    }

    /// Stands in for a member's durable accelerator sidecar, so a test can read
    /// back what the rebuild did (or did not) do to the persisted checkpoint.
    #[derive(Default)]
    struct MemoryPositionStore {
        inner: Mutex<Option<PersistedPosition>>,
    }

    #[async_trait]
    impl super::super::PositionStore for MemoryPositionStore {
        async fn load(
            &self,
        ) -> std::result::Result<Option<PersistedPosition>, super::super::StoreError> {
            Ok(lock(&self.inner).clone())
        }
        async fn save(
            &self,
            position: &PersistedPosition,
        ) -> std::result::Result<(), super::super::StoreError> {
            *lock(&self.inner) = Some(position.clone());
            Ok(())
        }
        async fn clear(&self) -> std::result::Result<(), super::super::StoreError> {
            *lock(&self.inner) = None;
            Ok(())
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    /// A source with one live member whose channel the test owns, built without
    /// touching `MySQL`: everything `rebootstrap_member` reads is local state.
    /// One live member and everything a test needs to observe what it emitted.
    struct TestMember {
        source: Arc<SharedSource>,
        member: Arc<MemberHandle>,
        position_store: Arc<MemoryPositionStore>,
        receiver: mpsc::Receiver<std::result::Result<ChangeEnvelope, StreamError>>,
    }

    fn source_with_member(member_key: &MemberKey) -> TestMember {
        let params = test_params();
        let source = Arc::new(SharedSource {
            key: SourceKey {
                host: "localhost".to_string(),
                port: 3306,
                user: "user".to_string(),
                pass: None,
                ssl: None,
                server_id: params.server_id,
            },
            params: params.clone(),
            setup_lock: tokio::sync::Mutex::new(()),
            members: Mutex::new(HashMap::new()),
            ack: Arc::new(AckTable::default()),
            pump_started: AtomicBool::new(true),
            restart_requested: AtomicBool::new(false),
            dead: AtomicBool::new(false),
            detached: Mutex::new(HashSet::new()),
        });
        let (sender, receiver) = mpsc::channel(8);
        let position_store: Arc<MemoryPositionStore> = Arc::new(MemoryPositionStore::default());
        let member = Arc::new(MemberHandle {
            dataset_name: "orders".to_string(),
            schema: test_schema(),
            primary_keys: vec!["id".to_string()],
            layout: Mutex::new(Arc::new(MemberLayout {
                // Covers every column of `test_schema`, so anything the member
                // builds from the layout (a truncate row included) is buildable
                // — an under-covered fixture would fail a test for its own
                // reasons rather than the asserted one.
                layout: super::super::setup::TableLayout {
                    columns: vec![
                        super::super::setup::SourceColumn {
                            name: "id".to_string(),
                            column_type: "bigint".to_string(),
                            enum_variants: None,
                            set_variants: None,
                            is_primary_key: true,
                        },
                        super::super::setup::SourceColumn {
                            name: "name".to_string(),
                            column_type: "varchar(64)".to_string(),
                            enum_variants: None,
                            set_variants: None,
                            is_primary_key: false,
                        },
                    ],
                },
                column_map: vec![0, 1],
                pk_source_indexes: vec![0],
            })),
            sender,
            metrics: MetricsCollector::new(),
            ready_lag: params.ready_lag,
            position_store: Arc::clone(&position_store) as Arc<dyn PositionStore>,
            checkpoint_schema_json: None,
            cursor_type: CursorType::File,
        });
        lock(&source.members).insert(member_key.clone(), Arc::clone(&member));
        // A streaming member: the rebuild is what puts it back into SNAPSHOTTING.
        source
            .ack
            .register(member_key, pos("binlog.000001", 100), false);
        source.ack.promote_ready_members();
        TestMember {
            source,
            member,
            position_store,
            receiver,
        }
    }

    #[tokio::test]
    async fn rebuild_replaces_the_acceleration_instead_of_emptying_it() {
        // A purged binlog position under `invalid_checkpoint_behavior: restart`
        // must not clear the acceleration and refill it through the change
        // stream: for the whole re-read, queries would be answered from an empty,
        // then partially filled, table. The member asks the consumer to replace
        // the contents atomically instead, so a query issued mid-rebuild returns
        // either the pre-rebuild rows or the completed ones.
        let member_key = key("db", "orders");
        let TestMember {
            source,
            member,
            position_store,
            mut receiver,
        } = source_with_member(&member_key);
        // The purged checkpoint this member is recovering from.
        let purged = PersistedPosition {
            position: pos("binlog.000001", 100),
            schema_json: None,
            gtid_set: None,
            cursor_type: CursorType::File,
        };
        position_store
            .save(&purged)
            .await
            .expect("seed the purged checkpoint");

        rebootstrap_member(
            &source,
            &member_key,
            &member,
            &pos("binlog.000004", 4),
            &GtidSet::new(),
        )
        .await
        .expect("rebootstrap the member");

        let envelope = receiver
            .try_recv()
            .expect("the rebuild signal is sent")
            .expect("the rebuild signal is not a stream error");
        assert!(
            envelope.history_unavailable(),
            "the member must ask the consumer to rebuild from the source"
        );
        assert!(
            !envelope.is_no_op_heartbeat(),
            "the signal carries the boundary committer, so the consumer's heartbeat \
             stripping cannot drop it and leave the new head unpersisted"
        );
        assert!(
            envelope.is_empty(),
            "the rebuild is a zero-row signal: no truncate, and no snapshot rows \
             for the consumer to apply on top of a table it is about to replace"
        );
        assert!(
            receiver.try_recv().is_err(),
            "the rebuild signal is the whole prelude — nothing follows it on the \
             member's channel"
        );

        // Held SNAPSHOTTING until the signal's committer runs, so the shared floor
        // cannot advance onto rows the consumer has not applied.
        let slot = source.ack.slot(&member_key).expect("member slot");
        assert!(slot.has(SNAPSHOTTING));
        assert_eq!(slot.committed(), pos("binlog.000004", 4));

        // The unusable checkpoint stays until the signal's committer replaces it
        // with the new head — see `rebootstrap_member` for why.
        let still_there = position_store
            .load()
            .await
            .expect("store readable")
            .expect("the purged checkpoint survives until the rebuild commits");
        assert_eq!(still_there.position, purged.position);
    }

    #[tokio::test]
    async fn only_the_rebuild_boundary_asks_for_a_replacement() {
        // The same envelope serves both loading starts; a first load has nothing
        // to preserve and must NOT trigger a rebuild — that would re-read the
        // source table on every cold start.
        let member_key = key("db", "orders");
        let TestMember { source, member, .. } = source_with_member(&member_key);

        for (history_unavailable, expected) in [(false, false), (true, true)] {
            let envelope = snapshot_boundary_envelope(
                &source,
                &member_key,
                &member.schema,
                member.dataset_name.clone(),
                history_unavailable,
            )
            .expect("build the boundary envelope");
            assert_eq!(envelope.history_unavailable(), expected);
            // Both carry the boundary committer: a snapshot's head must survive
            // the consumer's heartbeat stripping too.
            assert!(!envelope.is_no_op_heartbeat());
            assert!(!envelope.is_dataset_ready());
        }
    }

    #[test]
    fn both_loading_starts_hold_the_member_snapshotting() {
        // The ack floor is held for whichever way the acceleration is loaded. A
        // rebuild that reported itself as not-loading would let the shared floor
        // advance past rows the consumer has not applied yet.
        assert!(MemberStart::Snapshot.is_loading());
        assert!(MemberStart::Rebuild.is_loading());
        assert!(!MemberStart::Stream.is_loading());
    }

    #[test]
    fn gtid_checkpoint_resumes_when_subset_of_source() {
        // Normal restart: the source kept the checkpoint's history and grew.
        let checkpoint = gtids(&format!("{SRC_A}:1-100"));
        let source = gtids(&format!("{SRC_A}:1-150"));
        assert_eq!(
            gtid_checkpoint_verdict(&checkpoint, &source),
            CheckpointVerdict::Resume
        );
        // An empty checkpoint (gtid_mode = ON, zero txns applied) always resumes.
        assert_eq!(
            gtid_checkpoint_verdict(&GtidSet::new(), &source),
            CheckpointVerdict::Resume
        );
    }

    #[test]
    fn gtid_checkpoint_unresumable_after_reset_or_divergence() {
        let checkpoint = gtids(&format!("{SRC_A}:1-100"));

        // RESET MASTER / rebuilt server: source executed set under a new UUID.
        let rebuilt = gtids(&format!("{SRC_B}:1-3"));
        assert!(matches!(
            gtid_checkpoint_verdict(&checkpoint, &rebuilt),
            CheckpointVerdict::Unresumable(_)
        ));

        // Freshly reset GTID server with zero transactions applied.
        let empty_source = GtidSet::new();
        assert!(matches!(
            gtid_checkpoint_verdict(&checkpoint, &empty_source),
            CheckpointVerdict::Unresumable(_)
        ));

        // Divergence: same UUID, but the source is behind the checkpoint.
        let behind = gtids(&format!("{SRC_A}:1-50"));
        assert!(matches!(
            gtid_checkpoint_verdict(&checkpoint, &behind),
            CheckpointVerdict::Unresumable(_)
        ));
    }

    #[test]
    fn file_checkpoint_verdict_tracks_presence() {
        assert_eq!(file_checkpoint_verdict(true), CheckpointVerdict::Resume);
        // Purged, or a reset that restarted binlog numbering below the file.
        assert!(matches!(
            file_checkpoint_verdict(false),
            CheckpointVerdict::Unresumable(_)
        ));
    }

    /// A real `ROTATE` closing `binlog.000041` at ~1 GiB and opening
    /// `binlog.000042`: the stream continues at offset 4 of the *new* file, not
    /// at the *closed* file's end offset (which the event header carries).
    #[test]
    fn rotate_targets_the_new_files_resume_offset() {
        let rotate = RotateEvent::new(4, &b"binlog.000042"[..]);
        assert_eq!(
            rotate_target(&rotate),
            Some(pos("binlog.000042", 4)),
            "a rotate must reposition to the offset its payload names"
        );
    }

    #[test]
    fn a_fake_rotate_does_not_reposition_the_stream() {
        // The artificial rotate at the head of a dump opens no new file.
        let rotate = RotateEvent::new(0, &b"binlog.000042"[..]);
        assert_eq!(rotate_target(&rotate), None);
    }

    /// Regression test for #12042.
    ///
    /// The pump credits idle streaming members the coordinate each event
    /// advances the stream to. When a `ROTATE` contributed the *closing* file's
    /// end offset under the *opening* file's name, an idle member's committed
    /// floor jumped ~1 GiB past the new file's real offsets and `deliver_commit`
    /// then dropped every following transaction as `already_committed` — with no
    /// error, no detach, and no backpressure warning, for the rest of the run.
    #[test]
    fn a_rotate_credit_does_not_suppress_the_new_files_commits() {
        let ack = AckTable::default();
        let member = key("tpcc", "warehouse");
        // An idle member caught up to the end of the file about to be closed.
        ack.register(&member, pos("binlog.000041", 1_073_741_800), false);
        ack.promote_ready_members();

        let rotate = RotateEvent::new(4, &b"binlog.000042"[..]);
        let target = rotate_target(&rotate).expect("a real rotate repositions the stream");
        ack.credit_idle(&target, None);

        assert_eq!(
            ack.committed(&member),
            Some(pos("binlog.000042", 4)),
            "the credit must land at the new file's start"
        );

        // The first transaction of the newly opened file must still be delivered.
        let next_commit = pos("binlog.000042", 1_182);
        let slot = ack.slot(&member).expect("registered member has a slot");
        assert!(
            !slot.already_committed(&next_commit),
            "commit at {next_commit} was suppressed as already-applied, so its rows are lost"
        );
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
        ack.credit_idle(&pos("binlog.000001", 900), None);
        assert_eq!(
            ack.committed(&key("db", "a")),
            Some(pos("binlog.000001", 100)),
            "held member must not be credited"
        );
        // Complete snapshot + promote, then credit.
        ack.snapshot_finished(&key("db", "a"));
        ack.promote_ready_members();
        ack.credit_idle(&pos("binlog.000001", 900), None);
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

    #[test]
    fn credit_idle_skips_members_with_inflight_envelopes() {
        // The idle safe-advance may only credit a member whose delivered floor
        // equals its committed floor (nothing in flight). A member with an
        // outstanding, not-yet-acked delivery must hold its own floor — crediting
        // it would ack past a change the consumer has not durably applied.
        let ack = AckTable::default();
        ack.register(&key("db", "a"), pos("binlog.000001", 100), false);
        ack.register(&key("db", "b"), pos("binlog.000001", 100), false);
        ack.promote_ready_members();

        // `a` has an in-flight envelope: delivered past committed.
        ack.slot(&key("db", "a"))
            .expect("slot")
            .deliver(&pos("binlog.000001", 300));
        ack.credit_idle(&pos("binlog.000001", 500), None);

        // `b` (idle) was credited to 500; `a` holds the floor at its committed
        // 100 because its outstanding delivery is not yet acked.
        assert_eq!(
            ack.committed(&key("db", "b")),
            Some(pos("binlog.000001", 500))
        );
        assert_eq!(
            ack.committed(&key("db", "a")),
            Some(pos("binlog.000001", 100)),
            "member with an in-flight envelope must not be credited"
        );
        assert_eq!(ack.flush_position(), Some(pos("binlog.000001", 100)));

        // The consumer acks `a`'s delivery; the shared floor advances.
        ack.slot(&key("db", "a"))
            .expect("slot")
            .commit(&pos("binlog.000001", 300));
        assert_eq!(ack.flush_position(), Some(pos("binlog.000001", 300)));

        // `a` is idle again — a later idle credit advances everyone.
        ack.credit_idle(&pos("binlog.000001", 600), None);
        assert_eq!(ack.flush_position(), Some(pos("binlog.000001", 600)));
    }

    #[test]
    fn replayed_older_commits_never_regress() {
        // On a reconnect from the shared min, a member ahead of the min re-sees
        // commits it already applied. `AckSlot::commit` is a monotonic-max, so an
        // older replayed commit is a no-op, and `already_committed` reports it as
        // applied so the pump suppresses the re-delivery.
        let ack = AckTable::default();
        ack.register(&key("db", "a"), pos("binlog.000001", 500), false);
        ack.promote_ready_members();
        let slot = ack.slot(&key("db", "a")).expect("slot");

        slot.commit(&pos("binlog.000010", 900));
        assert_eq!(slot.committed(), pos("binlog.000010", 900));

        // An older replayed commit must not regress the floor.
        slot.commit(&pos("binlog.000005", 100));
        assert_eq!(
            slot.committed(),
            pos("binlog.000010", 900),
            "an older replayed commit must not regress the floor"
        );

        // The replay is reported already-applied (suppressed), only a strictly
        // newer position is not.
        assert!(slot.already_committed(&pos("binlog.000005", 100)));
        assert!(slot.already_committed(&pos("binlog.000010", 900)));
        assert!(!slot.already_committed(&pos("binlog.000010", 901)));
    }

    #[tokio::test]
    async fn shared_committer_advances_floor_and_absorbs_same_slot() {
        let ack = AckTable::default();
        ack.register(&key("db", "a"), pos("binlog.000001", 100), false);
        ack.register(&key("db", "b"), pos("binlog.000001", 100), false);
        ack.promote_ready_members();
        let slot = ack.slot(&key("db", "a")).expect("slot");

        // `commit()` advances the member's committed floor to `flush_to` and
        // folds its GTIDs into the member's executed set (lockstep with position).
        let uuid = uuid::Uuid::from_u128(1);
        let committer = SharedPositionCommitter {
            slot: Arc::clone(&slot),
            flush_to: pos("binlog.000001", 500),
            dataset: "orders".into(),
            source_commit_ts_ms: Some(1),
            gtids: vec![(uuid, 7)],
        };
        committer.commit().await.expect("commit");
        assert_eq!(slot.committed(), pos("binlog.000001", 500));
        assert_eq!(slot.gtid_snapshot().to_string(), format!("{uuid}:7"));

        // `try_absorb` folds a later commit to the SAME slot, keeping the higher
        // position (monotonic-max) AND accumulating both transactions' GTIDs so
        // neither is dropped from the executed set.
        let mut first = SharedPositionCommitter {
            slot: Arc::clone(&slot),
            flush_to: pos("binlog.000001", 500),
            dataset: "orders".into(),
            source_commit_ts_ms: Some(1),
            gtids: vec![(uuid, 8)],
        };
        let later = SharedPositionCommitter {
            slot: Arc::clone(&slot),
            flush_to: pos("binlog.000001", 900),
            dataset: "orders".into(),
            source_commit_ts_ms: Some(2),
            gtids: vec![(uuid, 9)],
        };
        assert!(first.try_absorb(&later));
        assert_eq!(first.flush_to, pos("binlog.000001", 900));
        assert_eq!(first.source_commit_ts_ms, Some(2));
        assert_eq!(first.gtids, vec![(uuid, 8), (uuid, 9)]);
        // Committing the coalesced committer folds both GTIDs (7 already present
        // ⇒ the set coalesces to 7-9).
        first.commit().await.expect("commit");
        assert_eq!(slot.gtid_snapshot().to_string(), format!("{uuid}:7-9"));

        // A committer for a DIFFERENT slot is never absorbed.
        let other = SharedPositionCommitter {
            slot: ack.slot(&key("db", "b")).expect("slot"),
            flush_to: pos("binlog.000001", 999),
            dataset: "customers".into(),
            source_commit_ts_ms: Some(3),
            gtids: Vec::new(),
        };
        assert!(!first.try_absorb(&other));
        assert_eq!(
            first.flush_to,
            pos("binlog.000001", 900),
            "a rejected absorb must not change flush_to"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_member_stall_accrues_the_send_stalled_counter() {
        // The pump reads the dump socket for the whole group, so time spent
        // waiting on one member's full channel is time the socket goes
        // undrained — the thing that ends in an aborted dump (#12527). The
        // warning scrolls away; the counter is what names the dataset that will
        // trigger the next reconnect.
        //
        // The stall interval is a constant, so the virtual clock converts
        // cleanly: no production path here derives a sleep from a real-clock
        // read.
        let metrics = MetricsCollector::new();
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .send(Err(StreamError::External(
                "occupies the channel".to_string(),
            )))
            .await
            .expect("the channel starts empty");

        let deliver = {
            let metrics = Arc::clone(&metrics);
            let epoch = crate::cdc::shutdown_epoch();
            tokio::spawn(async move {
                deliver_to_member(
                    &metrics,
                    &sender,
                    Err(StreamError::External("must be delivered".to_string())),
                    epoch,
                    "test_dataset",
                )
                .await
            })
        };

        // Two stall intervals with the channel still full, then drain it so the
        // must-deliver completes rather than looping forever.
        tokio::time::sleep(MEMBER_SEND_STALL_WARN * 2 + Duration::from_millis(1)).await;
        let stalled = Metrics::new(Arc::clone(&metrics)).member_send_stalled_seconds_total();
        assert_eq!(
            stalled,
            MEMBER_SEND_STALL_WARN.as_secs() * 2,
            "each stall interval must accrue, so one long stall is distinguishable from none"
        );

        let _queued = receiver.recv().await.expect("the queued envelope");
        let outcome = deliver.await.expect("the delivery task");
        assert!(
            matches!(outcome, DeliverOutcome::Sent),
            "a stalled delivery must still be delivered once the sink drains"
        );
        assert_eq!(
            Metrics::new(metrics).member_send_stalled_seconds_total(),
            stalled,
            "a successful send must not accrue stall time"
        );
    }

    #[tokio::test]
    async fn deliver_to_member_reports_receiver_gone() {
        // A dropped receiver (dataset stream torn down) must surface as
        // `ReceiverGone` so the pump detaches the member and holds its floor,
        // never wedging on a dead channel.
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let outcome = deliver_to_member(
            &MetricsCollector::new(),
            &sender,
            Err(StreamError::External("x".to_string())),
            crate::cdc::shutdown_epoch(),
            "test_dataset",
        )
        .await;
        assert!(matches!(outcome, DeliverOutcome::ReceiverGone));
    }

    #[test]
    fn credit_idle_interleaving_never_over_credits() {
        // The load-bearing cross-thread invariant: the committer (consumer
        // thread) advances `committed` while the pump thread races `credit_idle`
        // ahead. `credit_idle` writes commit-before-deliver and only credits a
        // member it observed idle, so the shared floor can never run past what a
        // member has durably applied, and never regresses.
        const N: u64 = 20_000;
        let ack = Arc::new(AckTable::default());
        ack.register(&key("db", "a"), pos("binlog.000001", 1), false);
        ack.promote_ready_members();
        let slot = ack.slot(&key("db", "a")).expect("slot");
        let done = Arc::new(AtomicBool::new(false));

        std::thread::scope(|s| {
            // Committer: deliver then commit, strictly increasing. Signals `done`
            // on completion so the crediter's busy loop terminates.
            {
                let slot = Arc::clone(&slot);
                let done = Arc::clone(&done);
                s.spawn(move || {
                    for p in 2..=N {
                        slot.deliver(&pos("binlog.000001", p));
                        slot.commit(&pos("binlog.000001", p));
                    }
                    done.store(true, Ordering::Release);
                });
            }
            // Idle-crediter: race `upto` ahead of the committer until it's done.
            {
                let ack = Arc::clone(&ack);
                let done = Arc::clone(&done);
                s.spawn(move || {
                    let upto = pos("binlog.000001", N + 1000);
                    while !done.load(Ordering::Acquire) {
                        ack.credit_idle(&upto, None);
                    }
                });
            }
            // Observer: (1) the shared floor never exceeds the member's own
            // committed position (acking past durable apply is the data-loss bug
            // this design guards against), and (2) the floor never regresses. Read
            // the floor BEFORE committed: both monotonic, so a later committed
            // read is >= the value the floor was computed from — a floor that
            // jumped past a real in-flight commit is thus observable, not masked.
            {
                let ack = Arc::clone(&ack);
                let slot = Arc::clone(&slot);
                s.spawn(move || {
                    let mut last_floor = pos("binlog.000001", 0);
                    for _ in 0..200_000 {
                        let floor = ack.flush_position().expect("one member");
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

        // Quiescence: `credit_idle` may legitimately have advanced the idle member
        // up to its `upto` (N+1000), so `committed` lands in [N, N+1000]; the floor
        // (single member ⇒ its committed) must have converged to it.
        let final_committed = slot.committed();
        assert!(
            (N..=N + 1000).contains(&final_committed.pos),
            "committed {final_committed} outside any writer's range"
        );
        assert_eq!(
            ack.flush_position(),
            Some(final_committed),
            "floor converged to the member's committed"
        );
    }
}
