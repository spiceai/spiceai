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
pub mod echo;
pub mod metrics;
pub mod pgoutput;
pub mod resilience;
pub mod retention;
pub mod schema_evolution;
pub mod shared;
pub mod slot;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use snafu::Snafu;

use crate::cdc::{ChangesStream, StreamError};

pub use config::{ReplicationParams, SchemaEvolutionPolicy};
pub use echo::{XactStatus, XidEntry, XidRegistry};
pub use metrics::{Metrics as ReplicationMetrics, MetricsCollector as ReplicationMetricsCollector};
pub use pgwire_replication::{CaCertificate, PgOutputFormat};
pub use retention::{SlotRemoval, SlotRetentionPosture};
pub use slot::{SlotInfo, SlotSetupOutcome};

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

/// Whether the acceleration must be rebuilt from the source rather than resumed.
///
/// The whole gap decision, as arithmetic rather than inference:
///
/// * `watermark` — the LSN the acceleration's contents are complete as of, or
///   `None` when none has been recorded.
/// * `slot_earliest_streamable_lsn` — the earliest LSN the slot can still stream
///   from, or `None` when the slot does not exist. This is the later of its
///   `restart_lsn` and its `confirmed_flush_lsn`, not `restart_lsn` alone:
///   Postgres forwards a start position below `confirmed_flush_lsn` up to it, so
///   an acknowledged change cannot be re-streamed even while its WAL is retained.
/// * `absence_implies_gap` — whether a *missing* watermark is evidence of one.
///   True when the acceleration survives restarts (so it can hold rows this
///   process did not load), a position could have been recorded (so absence is
///   informative rather than permanent), and the acceleration is not known to be
///   empty (see below).
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
pub fn needs_rebuild(
    position: &RecordedPosition,
    slot_earliest_streamable_lsn: Option<u64>,
    absence_implies_gap: bool,
) -> bool {
    match position {
        // Nothing recorded: a gap only when absence is informative — see
        // `absence_implies_gap`.
        RecordedPosition::Absent => absence_implies_gap,
        // Recorded against a different source. Whatever the acceleration holds
        // came from somewhere else, and the LSN is not even comparable — a small
        // LSN from the new source would otherwise read as "already covered" and
        // leave the old source's rows in place while never loading the new
        // source's.
        RecordedPosition::ForeignSource => true,
        // A gap when there is no slot at all, or when the slot can no longer
        // stream from as far back as the watermark.
        RecordedPosition::At(watermark) => {
            slot_earliest_streamable_lsn.is_none_or(|earliest| earliest > watermark.lsn)
        }
    }
}

/// What the local record says about an acceleration's position.
///
/// Three outcomes rather than `Option`, because "recorded against a different
/// source" is neither "no record" nor a usable position: LSNs are only
/// comparable within one source's history, so a watermark carried over to a
/// different server, database, or table describes contents that have nothing to
/// do with what this dataset now streams.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordedPosition {
    /// Nothing recorded.
    Absent,
    /// A position exists, but for a different source than this dataset streams
    /// from now. Its LSN cannot be compared and its contents cannot be trusted.
    ForeignSource,
    /// A position recorded against this same source.
    At(AppliedLsn),
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
    /// changes) before they become Arrow. See `echo.rs` and
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
    use super::{AppliedLsn, RecordedPosition, needs_rebuild};

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

        // A position recorded against another source is never usable, whatever
        // the slot says and whatever the acceleration's durability: its LSN is
        // not comparable and its contents describe a different table.
        assert!(needs_rebuild(
            &RecordedPosition::ForeignSource,
            Some(0),
            true
        ));
        assert!(needs_rebuild(
            &RecordedPosition::ForeignSource,
            Some(0),
            false
        ));
        assert!(needs_rebuild(&RecordedPosition::ForeignSource, None, false));

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
        // watermark of 100, even though 100 sits inside the retained range.
        let restart_lsn: u64 = 40;
        let confirmed_flush_lsn: u64 = 200;
        assert!(
            needs_rebuild(&at(100), Some(restart_lsn.max(confirmed_flush_lsn)), true),
            "a watermark behind the acknowledged position is unreachable"
        );
        assert!(
            !needs_rebuild(&at(100), Some(restart_lsn), true),
            "control: comparing against retention alone calls the same gap resumable"
        );
    }
}
