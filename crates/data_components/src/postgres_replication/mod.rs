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

//! `PostgreSQL` logical replication for Spice acceleration.
//!
//! Streams WAL changes from a source Postgres database directly into the local
//! accelerator via the existing [`crate::cdc::ChangesStream`] abstraction.
//!
//! Entry point: [`start_replication_stream`]. Compiled as part of the
//! `postgres` feature on this crate — no separate feature flag.

pub mod bootstrap;
pub mod changes;
pub mod client;
pub mod config;
pub mod metrics;
pub mod pgoutput;
pub mod resilience;
pub mod retention;
pub mod schema_evolution;
pub mod shared;
pub mod slot;
pub mod xid_registry;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use snafu::Snafu;

use crate::cdc::{ChangesStream, StreamError};

pub use config::{ReplicationParams, SchemaEvolutionPolicy};
pub use metrics::{Metrics as ReplicationMetrics, MetricsCollector as ReplicationMetricsCollector};
pub use pgwire_replication::{CaCertificate, PgOutputFormat};
pub use retention::{SlotRemoval, SlotRetentionPosture};
pub use slot::{SlotInfo, SlotSetupOutcome};
pub use xid_registry::{XactStatus, XidEntry, XidRegistry};

/// Extracts a human-readable message from a `tokio_postgres::Error`.
///
/// `tokio_postgres::Error`'s `Display` impl for DB errors outputs the opaque
/// string "db error", hiding the actual `PostgreSQL` server message. This helper
/// surfaces the severity + message (and detail, if present) from the underlying
/// `DbError` so that log lines contain actionable text.
pub(crate) fn pg_error_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut msg = format!("{}: {}", db.severity(), db.message());
        if let Some(detail) = db.detail() {
            msg.push_str(" — ");
            msg.push_str(detail);
        }
        msg
    } else {
        e.to_string()
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to establish setup connection to Postgres: {}",
        pg_error_detail(source)
    ))]
    SetupConnect { source: tokio_postgres::Error },

    #[snafu(display("Failed to execute setup SQL: {}", pg_error_detail(source)))]
    SetupExec { source: tokio_postgres::Error },

    #[snafu(display(
        "PostgreSQL logical replication is not enabled on this server. \
         Set wal_level = 'logical' in postgresql.conf and restart PostgreSQL. \
         You can also run: ALTER SYSTEM SET wal_level = 'logical'; \
         then restart the server for the change to take effect."
    ))]
    LogicalReplicationNotEnabled,

    #[snafu(display(
        "Table {schema}.{table} has REPLICA IDENTITY NOTHING. Logical replication requires \
         REPLICA IDENTITY DEFAULT (primary key) or FULL. Run: \
         ALTER TABLE {schema}.{table} REPLICA IDENTITY FULL;"
    ))]
    UnsupportedReplicaIdentity { schema: String, table: String },

    #[snafu(display(
        "Table {schema}.{table} has no primary key and no REPLICA IDENTITY FULL. \
         Logical replication requires a primary key on the source table."
    ))]
    MissingPrimaryKey { schema: String, table: String },

    #[snafu(display("Source table {schema}.{table} does not exist"))]
    SourceTableNotFound { schema: String, table: String },

    #[snafu(display("Failed to start replication client: {source}"))]
    StartReplication {
        source: pgwire_replication::PgWireError,
    },

    #[snafu(display("pgoutput decode error: {message}"))]
    PgOutputDecode { message: String },

    #[snafu(display("Schema mismatch: {message}"))]
    SchemaMismatch { message: String },

    #[snafu(display("Bootstrap stream error: {}", pg_error_detail(source)))]
    Bootstrap { source: tokio_postgres::Error },

    #[snafu(display("Invalid Postgres LSN string `{lsn}`: expected `XXXXXXXX/YYYYYYYY`"))]
    InvalidLsn { lsn: String },

    #[snafu(display("Failed to build TLS configuration for Postgres replication: {source}"))]
    TlsConfig { source: config::TlsConfigError },

    #[snafu(display(
        "Table {schema}.{table} is already subscribed on shared replication slot `{slot}` by \
         another dataset. Each source table can back at most one dataset per shared slot — \
         give this dataset a different `pg_replication_slot` (or remove the param to get a \
         dedicated per-dataset slot)."
    ))]
    SharedTableAlreadySubscribed {
        schema: String,
        table: String,
        slot: String,
    },

    #[snafu(display(
        "Dataset `{dataset}` joins a shared replication slot whose publication is \
         `{expected}`, but declares publication `{got}`. All datasets sharing a slot must \
         use the same publication — remove the per-dataset `pg_publication` override or \
         make it consistent."
    ))]
    SharedPublicationMismatch {
        dataset: String,
        expected: String,
        got: String,
    },

    #[snafu(display(
        "Shared replication source for slot `{slot}` kept shutting down while this dataset \
         was subscribing. Check earlier log lines for the underlying stream failure."
    ))]
    SharedSourceUnavailable { slot: String },

    #[snafu(display(
        "Dataset `{dataset}` joins a shared replication slot but its `{param}` differs from \
         the dataset that opened the slot. All datasets sharing a slot are served over one \
         replication connection and must use identical connection parameters — make `{param}` \
         consistent, or give this dataset its own `pg_replication_slot`."
    ))]
    SharedConnectionParamsMismatch {
        dataset: String,
        param: &'static str,
    },

    #[snafu(display(
        "Dataset `{dataset}` cannot share replication slot `{slot}`: it needs a slot that \
         {joining}, but the dataset that opened the slot needs one that {existing}. A shared slot \
         has a single lifetime for every dataset on it — it is released when Spice shuts down, \
         and its WAL history is discarded when re-bootstrapping, but only when every dataset on \
         it both starts empty and re-runs its initial snapshot. Mixing the two would silently \
         drop changes for the dataset that relies on replaying that history. Give this dataset \
         its own `pg_replication_slot`, or match the acceleration `mode` and \
         `pg_replication_initial_snapshot` of every dataset sharing this slot. \
         See: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    SharedSlotDurabilityMismatch {
        dataset: String,
        slot: String,
        /// Pre-rendered so the message needs no conditional formatting, e.g.
        /// "is retained across restarts so its history can be replayed".
        joining: &'static str,
        existing: &'static str,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The LSN an acceleration's contents are complete as of, as recorded locally.
///
/// `PostgreSQL` CDC has historically kept no client-side position at all — it
/// relied on the slot's server-tracked `confirmed_flush_lsn`, which is precisely
/// what disappears when a slot is dropped or invalidated. Recording the position
/// locally (the same thing `MySQL` does with `spice_sys_mysql_binlog`) is what
/// lets startup *compute* whether the source can still supply the missing
/// changes instead of inferring it from slot and publication state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppliedLsn {
    /// Every change committed at or before this LSN is durably reflected in the
    /// acceleration.
    pub lsn: u64,
}

/// Why the acceleration must be rebuilt from the source, or `None` to resume.
///
/// The whole gap decision *and* which [`RebuildCause`] it is, together, so the
/// comparison that settles it is written once — deciding in one place and
/// explaining in another leaves two encodings of one rule, free to drift.
///
/// * `position` — what the local record says, see [`RecordedPosition`].
/// * `slot_restart_lsn` — the earliest LSN the slot still retains, or `None`
///   when the slot does not exist.
/// * `slot_acknowledged_lsn` — the later of the slot's `confirmed_flush_lsn` and
///   this member's own seated floor. Retention is not reachability: Postgres
///   forwards a start position below `confirmed_flush_lsn` up to it ("has been
///   already streamed, forwarding to ..." in `CreateDecodingContext`), so once a
///   slot has acknowledged past a change no client can ask for it again — the WAL
///   may still be on disk under `restart_lsn`, but it is unreachable through this
///   slot. A slot-mate's traffic can carry this member's own floor past the
///   slot's snapshot the same way. Comparing against retention alone calls an
///   unfillable gap resumable and skips the difference silently (#11289).
/// * `absence_implies_gap` — whether a *missing* watermark is evidence of one.
///   True when the acceleration survives restarts (so it can hold rows this
///   process did not load), a position could have been recorded (so absence is
///   informative rather than permanent), and the acceleration is not known to be
///   empty (see below).
/// * `emptiness_implies_gap` — whether an acceleration *observed to hold no
///   rows* is evidence of one. True when it is provably empty and nothing else
///   is going to load it. An emptied acceleration and a recorded position are
///   individually ordinary and jointly a gap: the position asserts every change
///   below it is already applied, so no reachable WAL will ever re-supply the
///   rows that are gone. See "Why an empty acceleration with a usable position
///   is a gap" below.
///
/// This is also where an ordinary backup or point-in-time restore is caught, in
/// two halves depending on when the process comes back. Reconnect before the
/// restored source has written anything and the stale position sits above its
/// WAL head, which [`recorded_position_is_ahead_of_source`] turns into
/// [`UnusableReason::RewoundSource`] before this is called. Reconnect later and
/// the check here does it instead: a restore leaves no logical slot behind, so
/// the one created on the next start takes `slot_acknowledged_lsn` from the WAL
/// head, above any pre-restore position. The two partition the comparison.
///
/// A watermark the slot cannot reach is a gap nothing can fill: the changes in
/// between are gone from the source's log, so a row deleted there would never be
/// deleted here. Rebuilding re-reads the table; resuming would keep it forever.
///
/// A *missing* watermark is not proof of an empty acceleration. It is also what
/// an acceleration written by a version that never recorded one looks like, and
/// what one whose watermark write failed looks like. Both can hold rows, and both
/// can be missing deletions for exactly the same reason. So a durable
/// acceleration with no recorded position is rebuilt rather than assumed fresh:
/// on a genuinely first load the rebuild reads the same rows the bootstrap would
/// have, and on an upgraded one it repairs divergence that is already there.
///
/// What settles that question is the acceleration's *contents*, not its record.
/// An acceleration observed to hold no rows has nothing that could be stale and
/// no deletion it could be missing, so absence of a watermark tells against
/// nothing and the load can proceed through the ordinary snapshot bootstrap.
/// Only a positive observation of emptiness counts
/// ([`crate::cdc::AccelerationContents::is_provably_empty`]); a probe that could
/// not answer leaves the rebuild in place.
///
/// Emptiness alone is not enough, and callers must not pass it through on its
/// own: it licenses skipping the rebuild only when a snapshot is actually going
/// to run. If nothing else loads the table, the rebuild is the only thing that
/// would, and skipping it resumes from the slot's position with every earlier
/// row missing for good.
///
/// # Why an empty acceleration with a usable position is a gap
///
/// The two halves of `absence_implies_gap` above read emptiness in the direction
/// that *licenses a resume*: an acceleration holding no rows has nothing stale
/// and no missing deletion, so a missing watermark tells against nothing. The
/// same observation against a watermark that *is* present says the opposite, and
/// only this arm reads it that way. A recorded position asserts that every change
/// below it has already been applied here, so the slot will never resend those
/// changes — however much WAL it retains. An acceleration that is nonetheless
/// empty is therefore missing every row committed before that position, with no
/// event left anywhere to supply them. Resuming completes without error and the
/// table stays permanently short of the source.
///
/// This is reachable without anything being broken. `mode: file_update` recreates
/// the acceleration when the source schema changes incompatibly
/// (`recreates_on_schema_mismatch`), which drops the accelerated table while the
/// watermark sidecar lives in the same accelerator and survives — so the next
/// start finds an empty table and a perfectly usable position. Restoring an older
/// accelerator file, or clearing the table by hand, lands in the same state.
///
/// Emptiness alone is not the gap — `emptiness_implies_gap` is false whenever a
/// snapshot is going to populate the table, which is the ordinary first load and
/// every re-snapshot after it. What makes it one is nothing else loading the
/// table, exactly as for a missing watermark.
///
/// A legitimately empty acceleration — every source row deleted, or retention
/// having aged them all out — is rebuilt too, and that is the intended trade
/// rather than an accepted false positive: the two states are indistinguishable
/// from here, the rebuild of a genuinely empty source reads nothing, and this
/// path already prefers a needless re-read to an unproven resume everywhere else.
/// Only a *positive* observation of emptiness counts
/// ([`crate::cdc::AccelerationContents::is_provably_empty`]), so a probe that
/// could not answer resumes exactly as it does today.
///
/// The slot-health causes keep their precedence over this one: a rebuild that
/// fires today keeps reporting the cause it reports today, and this arm only ever
/// names a rebuild where the position was otherwise about to be resumed.
///
/// # Why an unreachable watermark rebuilds even with snapshots disabled
///
/// `pg_replication_initial_snapshot: disabled` says "do not read the source table
/// to populate this acceleration", so answering slot loss with a full re-read
/// looks like it contradicts the setting. It does not, and the alternative is
/// worse. `disabled` is a preference about *initial load* — how the acceleration
/// comes into existence — not a licence to keep serving one that is known to have
/// diverged. Once the slot cannot stream from the recorded position, a row deleted
/// at the source in that window has no change event left to carry the deletion, so
/// every later query answers from rows the source no longer has.
///
/// The apparent alternative — make it terminal, and let an operator decide — does
/// not exist today: a fatal changes-stream error marks the dataset's status
/// `Error` but does not stop its accelerated table from serving, so "terminal"
/// means serving those same wrong rows indefinitely with a status field to say so.
/// Between a re-read the operator did not ask for and an answer that is wrong, the
/// re-read wins. Revisit only if a post-load fatal can make the table refuse a
/// scan (#13218); until then this arm is deliberate, not inherited.
#[must_use]
pub fn rebuild_cause(
    position: &RecordedPosition,
    slot_restart_lsn: Option<u64>,
    slot_acknowledged_lsn: u64,
    absence_implies_gap: bool,
    emptiness_implies_gap: bool,
) -> Option<RebuildCause> {
    match position {
        // Nothing recorded: a gap only when absence is informative — see
        // `absence_implies_gap`.
        RecordedPosition::Absent => absence_implies_gap.then_some(RebuildCause::NoRecord),
        // The record cannot be compared against this slot at all. Whatever the
        // acceleration holds is unproven, and the LSN is not usable as a
        // position: a small one would otherwise read as "already covered" and
        // leave stale rows in place while never loading the current ones.
        RecordedPosition::Unusable(UnusableReason::ForeignSource) => {
            Some(RebuildCause::ForeignSource)
        }
        RecordedPosition::Unusable(UnusableReason::Unreadable) => Some(RebuildCause::Unreadable),
        RecordedPosition::Unusable(UnusableReason::RewoundSource) => {
            Some(RebuildCause::RewoundSource)
        }
        // Acknowledgement before retention: it is the tighter limit and the more
        // misleading one, easily mistaken for a retention problem when the WAL is
        // still on disk.
        RecordedPosition::At(watermark) if slot_acknowledged_lsn > watermark.lsn => {
            Some(RebuildCause::AcknowledgedPast)
        }
        // No slot at all retains nothing, and a slot retaining only past the
        // position cannot replay the changes that follow it.
        RecordedPosition::At(watermark)
            if slot_restart_lsn.is_none_or(|restart_lsn| restart_lsn > watermark.lsn) =>
        {
            Some(RebuildCause::RetentionLost)
        }
        // The slot can serve the resume, but there is nothing here to resume onto:
        // the position says every change below it is applied, and the table is
        // observed empty. Placed last of the `At` arms so a rebuild that fires
        // today keeps the cause it reports today — this only names one the code
        // was otherwise about to resume.
        RecordedPosition::At(_) if emptiness_implies_gap => {
            Some(RebuildCause::EmptyWithUsablePosition)
        }
        RecordedPosition::At(_) => None,
    }
}

/// Why an acceleration was rebuilt from the source rather than resumed.
///
/// The causes are genuinely different situations calling for different
/// responses — a restored source, a repointed endpoint, a broken sidecar, a slot
/// lifecycle problem — so they are reported separately rather than as one
/// "rebuilt" event, in the log line and on the member's metrics alike.
///
/// Two accessors, deliberately: [`Self::label`] is an identifier that dashboards
/// and log queries match on and must stay stable, while [`Self::reason`] is prose
/// written for whoever reads the warning and is free to be reworded. One string
/// cannot be both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RebuildCause {
    /// No position was recorded, and its absence is informative — see
    /// `absence_implies_gap` on [`rebuild_cause`].
    NoRecord,
    /// The record names a different source than this dataset streams from now.
    ForeignSource,
    /// The record could not be read or parsed, so nothing about it is proven.
    Unreadable,
    /// The record names a position the source's history no longer contains,
    /// because it was restored or rewound afterwards.
    ///
    /// Worth alerting on rather than only counting, because one rewind escapes
    /// detection entirely: a slot that survived it still valid and at a
    /// pre-rewind position (a block-level snapshot of `PGDATA` restored as crash
    /// recovery), once the WAL has grown back past the recorded position.
    /// Catching that needs the last observed `confirmed_flush_lsn` persisted
    /// alongside the position — it only advances in normal operation, so a lower
    /// value on restart is proof the source went backwards. Timeline and system
    /// identifier do not settle it, since crash recovery neither promotes nor
    /// changes the identifier. So a dataset reporting this cause is reason to
    /// check whether others on the same source resumed when they should not have.
    RewoundSource,
    /// The slot acknowledged past the recorded position, so it can no longer be
    /// streamed from — even though its WAL may still be on disk.
    AcknowledgedPast,
    /// The slot no longer retains the WAL following the recorded position.
    RetentionLost,
    /// The acceleration was observed to hold no rows while recording a position
    /// the slot can still stream from, and nothing else is going to load it.
    ///
    /// Not a slot problem: the changes are still reachable, but the position
    /// asserts they were already applied, so they will never be resent. Reached
    /// by a `mode: file_update` recreate (which drops the table and leaves the
    /// watermark sidecar beside it), by a restored or hand-cleared accelerator
    /// file, and by a source whose rows were all legitimately deleted — the last
    /// of which rebuilds by reading nothing. See [`rebuild_cause`].
    EmptyWithUsablePosition,
}

impl RebuildCause {
    /// Stable identifier for metrics and log queries. Never reworded — renaming
    /// one breaks every dashboard and saved search that selects on it.
    ///
    /// A new variant lands here and in [`Self::reason`] by exhaustiveness; also
    /// add it to `causes` in
    /// `every_rebuild_cause_is_distinguishable_to_a_query_and_to_a_person`, which
    /// cannot notice one it was not given.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::NoRecord => "no_record",
            Self::ForeignSource => "foreign_source",
            Self::Unreadable => "unreadable",
            Self::RewoundSource => "rewound_source",
            Self::AcknowledgedPast => "acknowledged_past",
            Self::RetentionLost => "retention_lost",
            Self::EmptyWithUsablePosition => "empty_with_usable_position",
        }
    }

    /// The operator-facing clause, phrased to complete "this acceleration will be
    /// rebuilt from the source before changes are applied: ...".
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::NoRecord => {
                "it has no recorded position, so any rows it already holds cannot be shown to be current"
            }
            Self::ForeignSource => {
                "the position it recorded belongs to a different source, so it does not describe these rows"
            }
            Self::Unreadable => {
                "the position it recorded could not be read, so any rows it already holds cannot be shown to be current"
            }
            Self::RewoundSource => {
                "the position it recorded as applied is ahead of the source's current WAL position, so the source was restored or rewound after it was recorded and its contents do not describe the source's current history"
            }
            Self::AcknowledgedPast => {
                "the slot has been acknowledged past the position it recorded as applied, so the changes in between can no longer be streamed from it"
            }
            Self::RetentionLost => {
                "the slot no longer retains the changes following the position it recorded as applied"
            }
            Self::EmptyWithUsablePosition => {
                "it holds no rows while recording changes as already applied up to a position, so the changes below that position will never be resent and would stay missing here"
            }
        }
    }
}

/// Whether a recorded position lies ahead of the source's current WAL position.
///
/// Impossible within one server history: an applied position only ever comes
/// from a commit the server itself streamed, so its WAL can never sit behind
/// one. A position ahead of it identifies a source restored or rewound after the
/// position was recorded, and callers must downgrade such a record to
/// [`UnusableReason::RewoundSource`] — resuming on it keeps pre-restore rows, and
/// seating it as a replay floor would suppress post-restore changes below it.
///
/// Strictly greater, never equal: the last streamed commit's end LSN equals the
/// WAL position of a source that has been idle since, and that is a normal
/// resume.
///
/// This catches a restore only while the source has not written past the
/// recorded position yet; [`rebuild_cause`] documents the other half, and
/// [`RebuildCause::RewoundSource`] the rewind neither half sees.
#[must_use]
pub fn recorded_position_is_ahead_of_source(
    position: &RecordedPosition,
    current_wal_lsn: u64,
) -> bool {
    matches!(position, RecordedPosition::At(recorded) if recorded.lsn > current_wal_lsn)
}

/// What the local record says about an acceleration's position.
///
/// Three outcomes rather than `Option`, because "a record exists but cannot be
/// used" is neither "no record" nor a usable position: LSNs are only comparable
/// within one source's history, so a record that cannot be tied to the history
/// this dataset now streams describes contents that have nothing to do with it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordedPosition {
    /// Nothing recorded.
    Absent,
    /// A record exists but its position cannot be used — see [`UnusableReason`]
    /// for which. Never resumed on, and never seated as a replay-suppression
    /// floor.
    Unusable(UnusableReason),
    /// A position recorded against this same source, on a history the source
    /// still has.
    At(AppliedLsn),
}

/// Why a record's position cannot be used.
///
/// Carried on [`RecordedPosition::Unusable`] rather than collapsed into one
/// variant because all three rebuild for genuinely different reasons, and the
/// message that explains the rebuild has to name the right one — an operator
/// told their watermark "belongs to a different source" when it merely failed
/// to parse will go looking for the wrong problem.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnusableReason {
    /// Recorded against a different source than this dataset streams from now
    /// (a different server, database, or table), so its LSN is not comparable
    /// and its contents describe something else.
    ForeignSource,
    /// The record could not be read or parsed, so nothing about it is proven —
    /// including whether the acceleration is missing deletions.
    Unreadable,
    /// Recorded on a history the source no longer has: the position is ahead of
    /// the source's current WAL position, which only a restore or rewind can
    /// produce (see [`recorded_position_is_ahead_of_source`]).
    RewoundSource,
}

/// Durable, client-side record of how far a dataset's acceleration has been
/// advanced, so a restart can tell a resumable gap from an unfillable one.
///
/// Mirrors `mysql_replication::PositionStore`: implemented on the runtime side
/// over a `spice_sys` sidecar table (the replication layer cannot reach the
/// accelerator's own store), and supplied per dataset on
/// [`ReplicationStreamInput`].
#[async_trait::async_trait]
pub trait AppliedLsnStore: Send + Sync {
    /// Whether this store can actually record a position.
    ///
    /// A store that cannot (see [`NoopAppliedLsnStore`]) makes the *absence* of a
    /// watermark meaningless: it proves nothing about what the acceleration
    /// holds, and no start will ever record one. Rebuilding on absence would then
    /// re-read the whole table on every restart, forever, so absence is treated
    /// as it was before watermarks existed.
    fn records_positions(&self) -> bool {
        true
    }

    /// The recorded position — see [`RecordedPosition`] for why a record made
    /// against a different source is reported distinctly from no record at all.
    async fn load(
        &self,
    ) -> std::result::Result<RecordedPosition, Box<dyn std::error::Error + Send + Sync>>;
    /// Record `applied` as durably reflected in the acceleration. Called only
    /// after the corresponding changes are durable, never before.
    async fn save(
        &self,
        applied: AppliedLsn,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
    /// Forget the recorded position, so the next start treats the acceleration
    /// as never loaded.
    async fn clear(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// An [`AppliedLsnStore`] that persists nothing, for an acceleration that does
/// not survive a restart. There is no state a resumed position could be
/// consistent with — such an accelerator boots empty and re-snapshots every
/// start — so recording one would only invite a resume that skipped its rows.
pub struct NoopAppliedLsnStore;

#[async_trait::async_trait]
impl AppliedLsnStore for NoopAppliedLsnStore {
    fn records_positions(&self) -> bool {
        false
    }

    async fn load(
        &self,
    ) -> std::result::Result<RecordedPosition, Box<dyn std::error::Error + Send + Sync>> {
        Ok(RecordedPosition::Absent)
    }
    async fn save(
        &self,
        _applied: AppliedLsn,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
    async fn clear(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Input required to start a replication stream for a single dataset.
pub struct ReplicationStreamInput {
    /// Dataset name as it will appear in `ChangeBatch` commits.
    pub dataset_name: String,
    /// Parameters parsed from the connector's component-level params.
    pub params: ReplicationParams,
    /// Arrow schema of the source table, used to shape `ChangeBatch` data columns.
    pub schema: SchemaRef,
    /// Primary-key column names as declared on the dataset's acceleration config.
    pub primary_keys: Vec<String>,
    /// Schema-qualified table name being replicated (e.g. `public.users`).
    pub schema_name: String,
    pub table_name: String,
    /// Shared collector that the stream updates as it processes events.
    /// The connector reads this via its `MetricsProvider` to expose OpenTelemetry
    /// observables.
    pub metrics: Arc<ReplicationMetricsCollector>,
    /// The dataset's `on_schema_change` policy, mapped to a
    /// [`SchemaEvolutionPolicy`]. Drives the pump's per-member
    /// [`schema_evolution::RelationSchemaTracker`]: with a policy other than
    /// [`SchemaEvolutionPolicy::Block`], a mid-stream source column add / lossless
    /// type widening is adopted into the member's working schema so subsequent
    /// `ChangeBatch`es carry the wider data struct (which the runtime apply loop
    /// then reconciles against the accelerator). Callers set this directly (the
    /// connector maps the dataset's `on_schema_change`); [`start_replication_stream`]
    /// consumes it as-is, and [`start_replication_stream_with_policy`] overrides it.
    pub policy: SchemaEvolutionPolicy,
    /// Where this dataset's applied-LSN watermark is read and written.
    ///
    /// Supplied by the connector: a `spice_sys`-backed store for an acceleration
    /// that survives restarts, [`NoopAppliedLsnStore`] for one that does not.
    /// Startup compares the loaded watermark against what the slot can still
    /// supply to decide between resuming and rebuilding.
    pub applied_lsn_store: Arc<dyn AppliedLsnStore>,
    /// The dataset's outstanding-write-back-transaction registry, or `None` when
    /// the dataset does not deliver durable write-back.
    ///
    /// `Some` only for a durable-write-back dataset, and it is the **same**
    /// [`XidRegistry`] `Arc` the connector's delivery path registers into — the
    /// pump drops the echo of each registered transaction (the arbitrated table's
    /// changes) before they become Arrow. See `xid_registry.rs` and
    /// `cdc-echo-drop-xid-design.md`.
    pub write_back_registry: Option<Arc<XidRegistry>>,
}

/// Starts the bootstrap+WAL replication stream.
///
/// The returned stream yields change envelopes the same way the Debezium+Kafka
/// path does, so the rest of the refresh loop needs no changes.
///
/// Steps performed lazily on first poll:
///   1. Open a regular (non-replication) Postgres connection to set up the
///      publication and replication slot.
///   2. If this is a fresh slot, stream the source table's existing rows via a
///      `SELECT * FROM <table>` over a `REPEATABLE READ` transaction (not
///      `COPY` — row streaming).
///   3. Hand off to the `pgwire_replication::ReplicationClient` for streaming WAL.
///
/// Note: the initial snapshot is NOT tied to the slot's exported snapshot LSN
/// (that would require `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` via the
/// replication protocol). The consequence is at-least-once semantics across
/// the snapshot/WAL boundary — rows committed on the source between slot
/// creation and the start of the bootstrap transaction may be delivered twice.
/// The accelerator relies on PK-based upsert (`on_conflict: upsert`) to make
/// this safe. True exactly-once would require the exported-snapshot handshake
/// and is tracked as a follow-up.
///
/// Back-pressure: the returned stream waits for each envelope's `commit()` to
/// complete before emitting the next one, so the accelerator's write throughput
/// naturally paces the replication stream.
#[must_use]
pub fn start_replication_stream(input: ReplicationStreamInput) -> ChangesStream {
    // Uses the policy already set on `input` (the connector maps the dataset's
    // `on_schema_change`); construct with `policy: SchemaEvolutionPolicy::Block`
    // for the conservative default. Every dataset is served by the shared pump.
    shared::subscribe(input)
}

/// [`start_replication_stream`] with the dataset's `on_schema_change` policy.
///
/// With a policy other than [`SchemaEvolutionPolicy::Block`], pgoutput
/// `Relation` messages are reconciled against the working schema: added
/// columns (and lossless OID type widening) are adopted so subsequent
/// `ChangeBatch`es carry the wider data struct, nullable dataset columns
/// absent from the relation are null-filled (pre-evolution WAL replay), and
/// non-widening changes surface a clear actionable error. See
/// [`schema_evolution::RelationSchemaTracker`].
///
/// Every dataset is served by the shared pump ([`shared::subscribe`]) — a
/// dataset on its own slot is just a one-member source — so the policy is
/// carried on [`ReplicationStreamInput`] and reconciled by the shared pump's
/// per-member [`schema_evolution::RelationSchemaTracker`] for all datasets,
/// slot-sharing or not. `input.params.shared` governs only slot/publication
/// naming, not which pump runs.
#[must_use]
pub fn start_replication_stream_with_policy(
    mut input: ReplicationStreamInput,
    policy: SchemaEvolutionPolicy,
) -> ChangesStream {
    // Every dataset is served by the shared pump — a dataset on its own slot is
    // just a one-member source (see [`shared`]). This unifies the apply path so
    // the pgoutput streaming protocol, ack floor, and schema evolution have a
    // single implementation. `input.params.shared` still governs slot/publication
    // *naming* (slot-derived when a slot is named, per-dataset otherwise), not
    // which pump runs.
    input.policy = policy;
    shared::subscribe(input)
}

fn stream_error(err: &Error) -> StreamError {
    StreamError::External(err.to_string())
}

/// Helper for `async_stream::try_stream!` blocks to turn an [`Error`] into the
/// `StreamError` that the cdc machinery speaks. Takes the error by value so it
/// can be used directly as a function pointer in `Result::map_err`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "used as a function pointer in map_err; taking by reference would require a closure at every call site"
)]
pub(crate) fn err_to_stream(err: Error) -> StreamError {
    stream_error(&err)
}

#[cfg(test)]
mod tests {
    use super::{
        AppliedLsn, RebuildCause, RecordedPosition, UnusableReason, rebuild_cause,
        recorded_position_is_ahead_of_source,
    };

    /// A slot that reaches everything, so a case varies only what it is about.
    ///
    /// `emptiness_implies_gap` is held false here: these cases are about what the
    /// *record* proves, and an acceleration whose contents could not be placed is
    /// the state every one of them describes. The emptiness dimension is varied
    /// on its own in
    /// `an_empty_acceleration_holding_a_usable_position_is_a_gap_nothing_will_fill`.
    fn needs_rebuild(
        position: &RecordedPosition,
        slot_earliest_streamable_lsn: Option<u64>,
        absence_implies_gap: bool,
    ) -> bool {
        rebuild_cause(
            position,
            slot_earliest_streamable_lsn,
            0,
            absence_implies_gap,
            false,
        )
        .is_some()
    }

    /// The gap decision is the correctness hinge of the rebuild path: a wrong
    /// `false` resumes over rows the source has deleted (silent divergence, the
    /// bug this exists to fix), while a wrong `true` costs a needless re-read of
    /// the table. Each case is asserted individually rather than trusting the
    /// comparison to read correctly.
    #[test]
    fn a_watermark_the_slot_cannot_reach_is_the_only_thing_that_forces_a_rebuild() {
        let at = |lsn| RecordedPosition::At(AppliedLsn { lsn });
        // A recorded watermark makes durability irrelevant: the comparison alone
        // decides, so these cases are asserted against a persisting acceleration.
        let needs_rebuild_persist =
            |position: RecordedPosition, restart| needs_rebuild(&position, restart, true);

        // Nothing recorded, and the acceleration boots empty: a first bootstrap,
        // not a gap. Snapshot-and-append is exactly right.
        assert!(!needs_rebuild(&RecordedPosition::Absent, Some(100), false));
        assert!(
            !needs_rebuild(&RecordedPosition::Absent, None, false),
            "an ephemeral acceleration with no slot yet is still a first load"
        );

        // Nothing recorded, but the acceleration persists: it may be holding rows
        // from a version that never recorded a watermark, or from a start whose
        // watermark write failed — either can already be missing deletions.
        // Rebuilding costs a re-read on a genuinely first load and repairs the
        // rest, so absence must not be read as emptiness.
        assert!(needs_rebuild(&RecordedPosition::Absent, Some(100), true));
        assert!(needs_rebuild(&RecordedPosition::Absent, None, true));

        // A record whose position cannot be used is never usable, whatever the
        // slot says and whatever the acceleration's durability — and each way it
        // happens is reported as its own cause, because they call for different
        // responses.
        for (reason, expected) in [
            (UnusableReason::ForeignSource, RebuildCause::ForeignSource),
            (UnusableReason::Unreadable, RebuildCause::Unreadable),
            (UnusableReason::RewoundSource, RebuildCause::RewoundSource),
        ] {
            let unusable = RecordedPosition::Unusable(reason);
            assert!(needs_rebuild(&unusable, Some(0), true), "{reason:?}");
            assert!(needs_rebuild(&unusable, Some(0), false), "{reason:?}");
            assert!(needs_rebuild(&unusable, None, false), "{reason:?}");
            assert_eq!(
                rebuild_cause(&unusable, Some(0), 0, true, false),
                Some(expected),
                "{reason:?} must not be reported as another cause"
            );
        }

        // The slot still holds WAL from at or before the watermark, so the gap is
        // replayable: resume.
        assert!(
            !needs_rebuild_persist(at(100), Some(100)),
            "exactly reachable"
        );
        assert!(
            !needs_rebuild_persist(at(100), Some(40)),
            "slot reaches further back"
        );

        // The slot's earliest position is past the watermark: the changes in
        // between are gone from the source's log and cannot be replayed.
        assert!(
            needs_rebuild_persist(at(100), Some(101)),
            "one byte past is still a gap"
        );
        assert!(needs_rebuild_persist(at(100), Some(u64::MAX)));

        // No slot at all reaches nothing.
        assert!(needs_rebuild_persist(at(100), None));

        // An unreadable watermark is reported by the caller as position 0, which
        // must resolve to a rebuild against any real slot position rather than
        // being mistaken for "never loaded".
        assert!(needs_rebuild_persist(at(0), Some(1)));
        assert!(needs_rebuild_persist(at(0), None));
    }

    /// `needs_rebuild` takes no `snapshotting` argument, so this is structural
    /// rather than conditional — but it is the answer to a real question (whether
    /// `pg_replication_initial_snapshot: disabled` should make slot loss terminal
    /// instead), and structure is exactly what a later change could quietly undo.
    #[test]
    fn an_unreachable_watermark_rebuilds_even_where_snapshots_are_disabled() {
        let at = |lsn| RecordedPosition::At(AppliedLsn { lsn });
        // `disabled` reaches this decision as a durable acceleration whose
        // watermark the slot can no longer stream from — the same inputs as any
        // other acceleration, because whether an *initial* snapshot may run says
        // nothing about whether these rows are still current.
        assert!(
            needs_rebuild(&at(100), Some(101), true),
            "a diverged acceleration is re-read rather than served"
        );
        // And the setting does not make a resumable gap into a rebuild either:
        // where the slot still reaches the watermark, `disabled` resumes like
        // everything else and never re-reads the source.
        assert!(!needs_rebuild(&at(100), Some(100), true));
    }

    /// The caller passes the later of `restart_lsn` and `confirmed_flush_lsn`,
    /// because Postgres forwards a start position below `confirmed_flush_lsn` up to
    /// it. Retained WAL that the slot has already acknowledged is therefore *not*
    /// streamable, and treating it as such is a silent skip (#11289).
    #[test]
    fn an_acknowledged_change_is_a_gap_even_while_its_wal_is_retained() {
        let at = |lsn| RecordedPosition::At(AppliedLsn { lsn });
        // A slot retaining from 40 but acknowledged to 200 cannot supply a
        // watermark of 100, even though 100 sits inside the retained range — and
        // it must say so, because "acknowledged past" reads like a retention
        // failure and is not one.
        let restart_lsn: u64 = 40;
        let confirmed_flush_lsn: u64 = 200;
        assert_eq!(
            rebuild_cause(
                &at(100),
                Some(restart_lsn),
                confirmed_flush_lsn,
                true,
                false
            ),
            Some(RebuildCause::AcknowledgedPast),
            "the acknowledged limit must not be reported as lost retention"
        );
        assert_eq!(
            rebuild_cause(&at(100), Some(restart_lsn), 0, true, false),
            None,
            "control: comparing against retention alone calls the same gap resumable"
        );
    }

    /// An acceleration observed to hold no rows, against a position the slot can
    /// still stream from. Every arm here is a resume the slot would happily serve
    /// — that is the point: the gap is not in the WAL, it is that the position
    /// asserts the rows were already applied, so no reachable change will ever
    /// re-supply them.
    ///
    /// A slot reaching everything (`Some(0)` retained, nothing acknowledged) is
    /// held fixed so only the emptiness dimension varies. Without the fix, every
    /// `Some(...)` assertion below returns `None` and the acceleration resumes
    /// permanently short of the source (#13546).
    #[test]
    fn an_empty_acceleration_holding_a_usable_position_is_a_gap_nothing_will_fill() {
        let at = |lsn| RecordedPosition::At(AppliedLsn { lsn });
        // A reachable position on a durable acceleration: today's resume, and the
        // control the case below is measured against.
        let reachable = |emptiness_implies_gap| {
            rebuild_cause(&at(100), Some(0), 0, true, emptiness_implies_gap)
        };

        assert_eq!(
            reachable(false),
            None,
            "control: an acceleration whose contents are unproven still resumes, so the new arm \
             cannot be firing on the position alone"
        );
        assert_eq!(
            reachable(true),
            Some(RebuildCause::EmptyWithUsablePosition),
            "an empty acceleration recording changes as applied is missing every row below that \
             position, and no reachable WAL will resend them"
        );

        // Position 0 is still a position: it asserts nothing was applied, so an
        // empty acceleration agrees with it and there is nothing to rebuild for.
        // Guards against reading emptiness as a gap on a genuine first load that
        // has already recorded its starting point.
        assert_eq!(
            rebuild_cause(&at(0), Some(0), 0, true, true),
            Some(RebuildCause::EmptyWithUsablePosition),
            "a recorded position of 0 is treated no differently — it is the caller's \
             `snapshotting` gate, not the LSN's value, that says whether a load is coming"
        );

        // The slot-health causes are strictly more specific about what to go and
        // look at, and they fire today. Emptiness must not relabel them, or an
        // operator with a retention problem is sent to look at their accelerator.
        assert_eq!(
            rebuild_cause(&at(100), Some(40), 200, true, true),
            Some(RebuildCause::AcknowledgedPast),
            "an acknowledged-past slot keeps its cause when the acceleration is also empty"
        );
        assert_eq!(
            rebuild_cause(&at(100), Some(140), 0, true, true),
            Some(RebuildCause::RetentionLost),
            "a slot that lost the following WAL keeps its cause when the acceleration is also empty"
        );
        assert_eq!(
            rebuild_cause(&at(100), None, 0, true, true),
            Some(RebuildCause::RetentionLost),
            "no slot at all keeps its cause when the acceleration is also empty"
        );
        for reason in [
            UnusableReason::ForeignSource,
            UnusableReason::Unreadable,
            UnusableReason::RewoundSource,
        ] {
            let unusable = RecordedPosition::Unusable(reason);
            assert_ne!(
                rebuild_cause(&unusable, Some(0), 0, true, true),
                Some(RebuildCause::EmptyWithUsablePosition),
                "{reason:?} describes an unusable record, not a usable position, so emptiness must \
                 not take over its cause"
            );
        }

        // Absence is the other half of the rule and is governed by its own flag:
        // emptiness alone must not manufacture a cause for a record that is not
        // there, or a genuine first load with snapshots pending would rebuild.
        assert_eq!(
            rebuild_cause(&RecordedPosition::Absent, Some(0), 0, false, true),
            None,
            "a missing record stays the `absence_implies_gap` decision — an empty acceleration \
             with nothing recorded is a first load"
        );
    }

    /// `label` is matched on by dashboards and log queries, so two causes sharing
    /// one would silently merge unrelated incidents; `reason` is all an operator
    /// reads, so two sharing one sends them after the wrong problem. Neither may
    /// collide, and neither may be empty.
    ///
    /// A new variant must be added to `causes` below. `label`/`reason` are
    /// exhaustive matches, so adding one forces a visit to both — and this list
    /// is named in the comment there for the same reason: an unlisted variant
    /// would leave this test passing while asserting nothing about it.
    #[test]
    fn every_rebuild_cause_is_distinguishable_to_a_query_and_to_a_person() {
        let causes = [
            RebuildCause::NoRecord,
            RebuildCause::ForeignSource,
            RebuildCause::Unreadable,
            RebuildCause::RewoundSource,
            RebuildCause::AcknowledgedPast,
            RebuildCause::RetentionLost,
            RebuildCause::EmptyWithUsablePosition,
        ];
        for (i, cause) in causes.iter().enumerate() {
            assert!(!cause.label().is_empty(), "{cause:?} has no label");
            assert!(!cause.reason().is_empty(), "{cause:?} has no reason");
            // Labels are identifiers, not prose: whitespace means someone reworded
            // one into a sentence and broke every query selecting on it.
            assert!(
                cause
                    .label()
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '_'),
                "{cause:?} label is not a stable identifier: {}",
                cause.label()
            );
            for other in &causes[i + 1..] {
                assert_ne!(
                    cause.label(),
                    other.label(),
                    "{cause:?} and {other:?} share a label, so a query cannot separate them"
                );
                assert_ne!(
                    cause.reason(),
                    other.reason(),
                    "{cause:?} and {other:?} read identically to an operator"
                );
            }
        }
    }

    /// A recorded position ahead of the source's current WAL position identifies
    /// a source restored or rewound since the position was recorded — its history
    /// no longer contains the recorded position. `attach_member` downgrades such
    /// a record to [`UnusableReason::RewoundSource`], which always rebuilds and
    /// is never seated as a replay-suppression floor; without the downgrade,
    /// `needs_rebuild` alone reads the rewind as resumable (the slot's earliest
    /// position sits below the watermark) and the seated floor would silently
    /// suppress every legitimate post-restore change at or below it.
    #[test]
    fn a_watermark_ahead_of_the_source_wal_identifies_a_rewound_source() {
        let at = |lsn| RecordedPosition::At(AppliedLsn { lsn });

        // Ahead of the current WAL position: only a rewind can produce this.
        assert!(recorded_position_is_ahead_of_source(&at(900), 800));
        // Equal is a normal resume of an idle source: the last streamed commit's
        // end LSN is exactly the WAL position when nothing has happened since.
        assert!(!recorded_position_is_ahead_of_source(&at(800), 800));
        // Behind is the ordinary case.
        assert!(!recorded_position_is_ahead_of_source(&at(700), 800));
        // Positions that are not comparable are never "ahead" — they are already
        // handled as unusable in their own right.
        assert!(!recorded_position_is_ahead_of_source(
            &RecordedPosition::Absent,
            800
        ));
        assert!(!recorded_position_is_ahead_of_source(
            &RecordedPosition::Unusable(UnusableReason::ForeignSource),
            800
        ));

        // The trap the downgrade closes: a fresh post-restore slot sits below the
        // stale watermark, so `needs_rebuild` on its own would resume...
        assert!(!needs_rebuild(&at(900), Some(500), true));
        // ...while the downgraded record rebuilds, and — being no longer `At` —
        // can never be seated as a replay floor.
        assert!(needs_rebuild(
            &RecordedPosition::Unusable(UnusableReason::RewoundSource),
            Some(500),
            true
        ));
    }
}
