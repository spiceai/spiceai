/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{
    fmt::Display,
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime},
};

use parking_lot::Mutex;

pub mod config;
pub use config::{
    DEFAULT_READY_LAG, InitialSnapshotMode, InvalidCheckpointBehavior, heartbeat_interval,
};

use arrow::error::ArrowError;
use arrow::{
    array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use arrow_buffer::OffsetBuffer;
use async_trait::async_trait;
use futures::stream::BoxStream;
use snafu::prelude::*;

/// Process-wide CDC shutdown signal, as a monotonically increasing *epoch*.
///
/// Raised by the runtime at the *start* of graceful shutdown — before the
/// (potentially long) connection-drain phase — so CDC sources can release
/// their upstream resources immediately: a Postgres replication connection
/// holds a single-consumer slot, and releasing it at SIGTERM (instead of at
/// process exit) lets a replacement instance attach during a rolling deploy
/// rather than retrying against "replication slot is active".
///
/// An epoch (rather than a one-way flag) keeps multi-`Runtime` processes
/// working: test suites construct and shut down several `Runtime` instances
/// in one process, and streams started *after* a shutdown capture the new
/// epoch and are unaffected. A stream stops when the epoch advances past the
/// value it captured at start.
static CDC_SHUTDOWN_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Signal every currently-running CDC source in the process to stop and
/// release its upstream resources. Sources started afterwards are unaffected.
pub fn begin_shutdown() {
    CDC_SHUTDOWN_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Release);
}

/// The current shutdown epoch. Long-running CDC sources capture this at
/// stream start and stop once it changes.
#[must_use]
pub fn shutdown_epoch() -> u64 {
    CDC_SHUTDOWN_EPOCH.load(std::sync::atomic::Ordering::Acquire)
}

/// A stream of [`ChangeEnvelope`] items produced by a CDC connector.
///
/// # Readiness contract
///
/// It is the responsibility of each connector implementation to indicate when
/// the dataset can be considered ready for queries by producing at least one
/// [`ChangeEnvelope`] with [`ChangeEnvelope::is_dataset_ready`] returning
/// `true`. The runtime treats the dataset as not ready and rejects queries
/// (with `AccelerationNotReady`) until that signal arrives.
///
/// Connectors MUST emit a ready signal even when the source has no new data
/// (e.g. on restart with an already-populated accelerator, or against a quiet
/// topic). For sources that may stay quiet indefinitely, use
/// [`build_ready_signal_envelope`] to emit a zero-row, no-op envelope as soon
/// as the connector determines it has caught up to the source (for example,
/// when Kafka consumer lag reaches zero, or when a Postgres logical
/// replication slot resumes from an existing position).
///
/// Failing to honor this contract causes the runtime to wait for an event
/// that may never arrive — see <https://github.com/spiceai/spiceai/issues/5201>.
pub type ChangesStream = BoxStream<'static, Result<ChangeEnvelope, StreamError>>;

#[derive(Debug, Snafu)]
pub enum CommitError {
    #[snafu(display("Failed to commit CDC change to dataset: {source}"))]
    UnableToCommitChange {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Debug, Snafu)]
pub enum ChangeBatchError {
    #[snafu(display("Schema didn't match expected change batch format {detail} schema={schema}"))]
    SchemaMismatch { detail: String, schema: SchemaRef },
    #[snafu(display("Failed to process change data capture update: {source}"))]
    Arrow { source: ArrowError },
    #[snafu(display(
        "Deferred change batch is no longer available: it was already consumed, \
         or an earlier build attempt failed. If a build failed, the preceding \
         'Failed to build deferred change batch' error carries the underlying cause"
    ))]
    DeferredBatchConsumed,
    #[snafu(display("Failed to build deferred change batch: {message}"))]
    DeferredBuild { message: String },
}

#[derive(Debug)]
pub enum StreamError {
    #[cfg(any(feature = "debezium", feature = "kafka"))]
    /// Error from the Kafka client, such as failure to consume messages.
    Kafka(crate::kafka::Error),
    /// Error from Serde JSON, such as failure to serialize or deserialize data.
    SerdeJsonError(String),
    /// Error from Arrow Flight, such as failure during streaming or subscription.
    Flight(String),
    /// Error from the Arrow library, such as failure during batch processing or manipulation.
    Arrow(String),
    /// External error not originating from `ChangesStream` core logic, such as index processing failure.
    External(String),
    /// Error surfaced by a data-source connector's change stream: the connector
    /// names itself (`connector`) and attaches its own concrete error as the boxed
    /// `source` cause, keeping this CDC contract connector-agnostic.
    Connector {
        /// Static name of the connector that produced the error (e.g. `"DynamoDB"`),
        /// so logs that print only `{err}` still identify the source connector.
        connector: &'static str,
        /// The connector's concrete error, preserved as the chained cause.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            #[cfg(any(feature = "debezium", feature = "kafka"))]
            StreamError::Kafka(e) => Some(e),
            StreamError::Connector { source, .. } => Some(&**source),
            // String-carrying variants have no underlying `Error` source.
            _ => None,
        }
    }
}

impl From<ChangeBatchError> for StreamError {
    fn from(e: ChangeBatchError) -> Self {
        // A change-batch build failure (including a deferred build) is not a
        // core stream-transport error; surface it as an external error carrying
        // the actionable message so the dataset's stream fails visibly rather
        // than dropping the batch.
        StreamError::External(e.to_string())
    }
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(any(feature = "debezium", feature = "kafka"))]
            StreamError::Kafka(e) => write!(f, "Kafka error: {e}"),
            StreamError::SerdeJsonError(e) => write!(f, "Serde JSON error: {e}"),
            StreamError::Flight(e) => write!(f, "Arrow Flight error: {e}"),
            StreamError::Arrow(e) => write!(f, "Arrow error: {e}"),
            StreamError::External(e) => write!(f, "External error: {e}"),
            StreamError::Connector { connector, source } => {
                write!(f, "{connector} error: {source}")
            }
        }
    }
}

/// Allows to commit a change that has been processed.
#[async_trait]
pub trait CommitChange {
    async fn commit(&self) -> Result<(), CommitError>;

    /// Whether deferring this commit is crash-safe: the source can re-stream from
    /// its last durable checkpoint after a crash, so the source offset is advanced
    /// only after downstream durability. Defaults to `false` (conservative — never
    /// defer); overridden `true` only by committers backed by a replayable source
    /// checkpoint (e.g. a Postgres replication slot). Consumers that defer the
    /// commit behind a later durability fence (in-memory CDC tier) MUST gate that
    /// deferral on this returning `true`, or a crash could lose data that the
    /// source can no longer re-stream.
    fn supports_deferral(&self) -> bool {
        false
    }

    /// Fold `other` into `self`, returning whether it was absorbed. Used to
    /// coalesce a run of consecutive commits that target the same stream
    /// position into one before they are run (see the consumer's burst-apply and
    /// deferred-checkpoint drain). An implementor MUST be **infallible** and
    /// **order-insensitive** in [`Self::commit`] for this to be sound: absorbing
    /// re-orders and drops intermediate commits, keeping only the folded result.
    /// Order-sensitive or fallible committers (per-partition offsets, resume
    /// tokens) MUST NOT override this — the default refuses to fold, so they stay
    /// byte-identical.
    fn try_absorb(&mut self, _other: &dyn CommitChange) -> bool {
        false
    }

    /// Downcast hook enabling [`Self::try_absorb`] to recognise a compatible
    /// sibling. Default `None` = "not coalesce-identifiable"; only committers
    /// that override `try_absorb` need override this (to `Some(self)`).
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    /// Whether [`Self::commit`] is statically known to do nothing — there is no
    /// source position, token, or checkpoint to acknowledge. Consumers may run
    /// or drop such a committer at any point without ordering or durability
    /// constraints; the runtime relies on this to keep zero-row readiness
    /// heartbeats out of the CDC write/durability path (see
    /// [`ChangeEnvelope::is_no_op_heartbeat`]). Defaults to `false`
    /// (conservative: assume the commit has effects, preserving ordering); only
    /// genuinely effect-free committers such as [`NoOpCommitter`] should
    /// override this to `true`.
    fn is_no_op(&self) -> bool {
        false
    }
}

/// Destination-passing-style source of the change rows carried by a
/// [`ChangeEnvelope`].
///
/// A CDC source that can render its wire format straight into Arrow (e.g.
/// Postgres pgoutput) implements this so a multiplexed reader can *route* an
/// event to the right dataset without paying the O(rows × columns) Arrow-typing
/// and UTF-8 cost on its shared hot path: [`ChangeRows::build`] runs later, on
/// the per-dataset consumer. An already-built [`ChangeBatch`] implements it
/// trivially (blanket impl below) so existing connectors are unchanged.
///
/// The metadata methods (`num_rows_hint`, `encoded_len`, `source_commit_ts_ms`,
/// `is_heartbeat`) MUST be answerable *without* building, so the consumer can
/// make coalescing/metric decisions cheaply and pay the build only once.
pub trait ChangeRows: Send {
    /// Whether there are zero output rows. MUST be exact (never an over- or
    /// under-estimate), so callers can safely branch or assert on it — e.g.
    /// ready-signal / heartbeat detection, or skip-empty. Answerable without
    /// building.
    fn is_empty(&self) -> bool;

    /// Upper bound on the number of output rows, for builder sizing, coalescing
    /// counts, and metrics. MAY exceed the exact count (a primary-key-changing
    /// UPDATE expands to two rows, which is only known precisely after
    /// decoding); over-estimating affects pre-allocation only, never
    /// correctness. Use [`Self::is_empty`], not `num_rows_hint() == 0`, when the
    /// zero case must be exact.
    fn num_rows_hint(&self) -> usize;

    /// Best-effort encoded byte size, for the consumer's coalescing byte
    /// budget, computed without building the Arrow batch (e.g. the buffered
    /// wire size for a raw source).
    fn encoded_len(&self) -> usize;

    /// Newest source COMMIT timestamp among these rows (ms since the Unix
    /// epoch), or `None` if the source provides none.
    fn source_commit_ts_ms(&self) -> Option<i64>;

    /// Whether these rows are a zero-row heartbeat carrying only a fresher
    /// source timestamp (see [`ChangeBatch::is_heartbeat`]).
    fn is_heartbeat(&self) -> bool;

    /// Render the rows into a [`ChangeBatch`]. Consumes `self` — runs at most
    /// once, off the source's hot path. Fallible: per-row value typing can fail
    /// (e.g. an unmergeable unchanged-TOAST column under `REPLICA IDENTITY
    /// DEFAULT`), and the failure MUST surface on the dataset's stream rather
    /// than dropping or corrupting data.
    fn build(self: Box<Self>) -> Result<ChangeBatch, ChangeBatchError>;
}

/// Trivial [`ChangeRows`] for an already-built batch: metadata reads the batch,
/// `build` returns it. Keeps every existing (non-deferred) connector working
/// through the same envelope interface.
impl ChangeRows for ChangeBatch {
    fn is_empty(&self) -> bool {
        self.record.num_rows() == 0
    }
    fn num_rows_hint(&self) -> usize {
        self.record.num_rows()
    }
    fn encoded_len(&self) -> usize {
        self.record.get_array_memory_size()
    }
    fn source_commit_ts_ms(&self) -> Option<i64> {
        self.source_commit_ts_ms
    }
    fn is_heartbeat(&self) -> bool {
        ChangeBatch::is_heartbeat(self)
    }
    fn build(self: Box<Self>) -> Result<ChangeBatch, ChangeBatchError> {
        Ok(*self)
    }
}

/// Holds the change rows as a lazily-built batch: a [`ChangeRows`] source plus a
/// one-time cache of the built [`ChangeBatch`].
///
/// Metadata queries are served from the source *without* building; the first
/// `get`/`into_built` runs [`ChangeRows::build`] and caches the result. A build
/// failure is terminal for the batch (the source is consumed); a retry reports
/// the consumed source as an error rather than silently yielding no data.
struct LazyChangeBatch {
    built: OnceLock<ChangeBatch>,
    /// `Some` until consumed by the first (successful or failed) build. The
    /// mutex guards only the take/build handoff and is never held across an
    /// `.await`. (The build itself is synchronous CPU work; a caller that runs
    /// it on an async task still occupies that worker for the build's duration —
    /// see the `build` doc — this just means the *lock* adds no await-blocking.)
    source: Mutex<Option<Box<dyn ChangeRows>>>,
}

impl LazyChangeBatch {
    fn from_rows(source: Box<dyn ChangeRows>) -> Self {
        Self {
            built: OnceLock::new(),
            source: Mutex::new(Some(source)),
        }
    }

    fn ready(batch: ChangeBatch) -> Self {
        // Pre-populate `built` so an eagerly-built envelope (every non-deferred
        // connector — Kafka/MongoDB/DynamoDB/Debezium/MySQL, ready signals) reads
        // metadata and the batch itself lock-free via `built.get()`, never boxing
        // the batch or dispatching through `source`.
        let built = OnceLock::new();
        let _ = built.set(batch);
        Self {
            built,
            source: Mutex::new(None),
        }
    }

    /// Return the built batch, running the deferred build on first access.
    /// Idempotent: a second call returns the cached batch.
    fn get(&self) -> Result<&ChangeBatch, ChangeBatchError> {
        if let Some(batch) = self.built.get() {
            return Ok(batch);
        }
        let mut source = self.source.lock();
        // Another caller may have built it while we waited on the lock.
        if let Some(batch) = self.built.get() {
            return Ok(batch);
        }
        let src = source.take().context(DeferredBatchConsumedSnafu)?;
        let batch = src.build()?;
        // `set` cannot fail: we hold the source lock and re-checked `built` is
        // empty above, so no other thread can have set it.
        let _ = self.built.set(batch);
        self.built.get().context(DeferredBatchConsumedSnafu)
    }

    /// Whether the batch is already built — an eager envelope, or a deferred
    /// one whose build has already run — so [`Self::into_built`] and the
    /// metadata accessors resolve without running a (possibly expensive)
    /// deferred build. Lets callers skip a `spawn_blocking` offload they'd
    /// only pay overhead for.
    fn is_materialized(&self) -> bool {
        self.built.get().is_some()
    }

    /// Consume into the owned built batch, building if needed.
    fn into_built(self) -> Result<ChangeBatch, ChangeBatchError> {
        if let Some(batch) = self.built.into_inner() {
            return Ok(batch);
        }
        let src = self
            .source
            .into_inner()
            .context(DeferredBatchConsumedSnafu)?;
        src.build()
    }

    // No-build metadata accessors: read the built batch directly (lock-free) if
    // present, else the not-yet-built source; the `default` covers the consumed
    // state (post-failed-build). Kept as separate methods rather than a shared
    // higher-order helper — the built and source branches borrow at different
    // lifetimes, which a single `FnOnce(&dyn ChangeRows)` helper can't satisfy.

    fn is_empty(&self) -> bool {
        if let Some(b) = self.built.get() {
            return b.record.num_rows() == 0;
        }
        // `is_some_and`, not `is_none_or`: a `None` source is only reachable
        // after a failed deferred build (a successful build populates `built`
        // and short-circuits above). That's an error state, not an empty batch —
        // report not-empty so a caller can't treat a failed envelope as a
        // skippable empty one and swallow the error.
        self.source
            .lock()
            .as_deref()
            .is_some_and(ChangeRows::is_empty)
    }

    fn num_rows_hint(&self) -> usize {
        if let Some(b) = self.built.get() {
            return b.record.num_rows();
        }
        self.source
            .lock()
            .as_deref()
            .map_or(0, ChangeRows::num_rows_hint)
    }

    fn encoded_len(&self) -> usize {
        if let Some(b) = self.built.get() {
            return b.record.get_array_memory_size();
        }
        self.source
            .lock()
            .as_deref()
            .map_or(0, ChangeRows::encoded_len)
    }

    fn source_commit_ts_ms(&self) -> Option<i64> {
        if let Some(b) = self.built.get() {
            return b.source_commit_ts_ms();
        }
        self.source
            .lock()
            .as_deref()
            .and_then(ChangeRows::source_commit_ts_ms)
    }

    fn is_heartbeat(&self) -> bool {
        if let Some(b) = self.built.get() {
            return b.is_heartbeat();
        }
        self.source
            .lock()
            .as_deref()
            .is_some_and(ChangeRows::is_heartbeat)
    }
}

pub struct ChangeEnvelope {
    change_committer: Box<dyn CommitChange + Send + Sync>,
    change_batch: LazyChangeBatch,
    is_dataset_ready: bool,
}

impl ChangeEnvelope {
    #[must_use]
    pub fn new(
        change_committer: Box<dyn CommitChange + Send + Sync>,
        change_batch: ChangeBatch,
        is_dataset_ready: bool,
    ) -> Self {
        Self {
            change_committer,
            change_batch: LazyChangeBatch::ready(change_batch),
            is_dataset_ready,
        }
    }

    /// Construct an envelope whose [`ChangeBatch`] is produced lazily from a
    /// [`ChangeRows`] source the first time it is accessed (via
    /// [`Self::change_batch`], [`Self::materialize`], or [`Self::into_parts`]),
    /// rather than built up front.
    ///
    /// Use this from a multiplexed CDC source to keep the shared read/route
    /// path free of per-row Arrow-typing cost; the build then runs on the
    /// per-dataset consumer. `rows` must own its inputs.
    #[must_use]
    pub fn new_from_rows(
        change_committer: Box<dyn CommitChange + Send + Sync>,
        rows: Box<dyn ChangeRows>,
        is_dataset_ready: bool,
    ) -> Self {
        Self {
            change_committer,
            change_batch: LazyChangeBatch::from_rows(rows),
            is_dataset_ready,
        }
    }

    /// Whether there are zero output rows, exactly, without forcing a build.
    /// See [`ChangeRows::is_empty`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.change_batch.is_empty()
    }

    /// Upper-bound output-row count without forcing a build, for sizing/metrics.
    /// See [`ChangeRows::num_rows_hint`].
    #[must_use]
    pub fn num_rows_hint(&self) -> usize {
        self.change_batch.num_rows_hint()
    }

    /// Encoded byte size without forcing a build, for the consumer's coalescing
    /// budget. See [`ChangeRows::encoded_len`].
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        self.change_batch.encoded_len()
    }

    /// Newest source commit timestamp (ms since Unix epoch) without forcing a
    /// build. See [`ChangeRows::source_commit_ts_ms`].
    #[must_use]
    pub fn source_commit_ts_ms(&self) -> Option<i64> {
        self.change_batch.source_commit_ts_ms()
    }

    /// Whether this is a zero-row heartbeat, without forcing a build. See
    /// [`ChangeRows::is_heartbeat`].
    #[must_use]
    pub fn is_heartbeat(&self) -> bool {
        self.change_batch.is_heartbeat()
    }

    /// Whether this envelope is a pure readiness heartbeat: a zero-row change
    /// batch whose committer is a no-op ([`CommitChange::is_no_op`]).
    ///
    /// CDC connectors emit these ([`build_heartbeat_envelope`],
    /// [`build_ready_signal_envelope`]) only to carry `is_dataset_ready` and a
    /// source freshness timestamp; they hold no data and acknowledge no source
    /// position, so a consumer may honor the ready flag and drop the envelope
    /// without writing, committing, or forcing any durability transition. A
    /// zero-row envelope whose committer has real effects (e.g. a `MySQL`
    /// snapshot-boundary envelope that persists the initial resume token) is
    /// NOT a heartbeat under this predicate and must keep normal
    /// durability-then-commit ordering.
    #[must_use]
    pub fn is_no_op_heartbeat(&self) -> bool {
        self.change_committer.is_no_op() && self.is_heartbeat()
    }

    pub async fn commit(self) -> Result<(), CommitError> {
        self.change_committer.commit().await
    }

    /// Borrow the change batch, building a deferred batch on first access.
    ///
    /// Returns an error if the deferred build fails (e.g. a per-row value that
    /// cannot be typed to the dataset schema); callers MUST surface it on the
    /// dataset's changes stream rather than skipping the batch.
    pub fn change_batch(&self) -> Result<&ChangeBatch, ChangeBatchError> {
        self.change_batch.get()
    }

    /// Consume the envelope into its parts, building a deferred batch if needed.
    ///
    /// The build is synchronous CPU work — for a deferred envelope under a
    /// large burst it can run well past the async runtime's ~100µs-per-await
    /// budget. Prefer [`Self::into_parts_offloaded`] from an async task; reserve
    /// this for synchronous contexts or already-materialized envelopes.
    pub fn into_parts(self) -> Result<ChangeEnvelopeParts, ChangeBatchError> {
        let batch = self.change_batch.into_built()?;
        Ok((self.change_committer, batch, self.is_dataset_ready))
    }

    /// [`Self::into_parts`] for async callers: a *deferred* envelope's
    /// synchronous Arrow build is offloaded to a blocking thread so it cannot
    /// stall the async worker (and, with it, `/health`) under a large burst.
    ///
    /// An already-materialized (eager) envelope resolves inline — its build is
    /// a no-op, and `spawn_blocking` dispatch would only add overhead to that
    /// hot path.
    pub async fn into_parts_offloaded(self) -> Result<ChangeEnvelopeParts, ChangeBatchError> {
        let materialized = self.change_batch.is_materialized();
        offload_build(materialized, move || self.into_parts()).await
    }

    /// Whether the change batch is already built, so [`Self::into_parts`]
    /// resolves without running a deferred build.
    fn is_materialized(&self) -> bool {
        self.change_batch.is_materialized()
    }

    #[must_use]
    pub fn from_parts(
        change_committer: Box<dyn CommitChange + Send + Sync>,
        change_batch: ChangeBatch,
        is_dataset_ready: bool,
    ) -> Self {
        Self {
            change_committer,
            change_batch: LazyChangeBatch::ready(change_batch),
            is_dataset_ready,
        }
    }

    /// Returns `true` if processing this envelope means the dataset can be
    /// marked ready for queries.
    ///
    /// See the [`ChangesStream`] documentation for the connector readiness
    /// contract.
    #[must_use]
    pub fn is_dataset_ready(&self) -> bool {
        self.is_dataset_ready
    }
}

/// The parts of a consumed [`ChangeEnvelope`]: committer, built change batch,
/// and dataset-ready flag.
pub type ChangeEnvelopeParts = (Box<dyn CommitChange + Send + Sync>, ChangeBatch, bool);

/// Run a CDC batch build off the async worker, but only when it would actually
/// block: an already-materialized build is a no-op, and `spawn_blocking`
/// dispatch would be pure overhead on that hot path.
///
/// Shared by both entry points so the offload policy — when to hand off, and how
/// a lost blocking task maps onto [`ChangeBatchError`] — is decided in one place.
async fn offload_build<T: Send + 'static>(
    materialized: bool,
    build: impl FnOnce() -> Result<T, ChangeBatchError> + Send + 'static,
) -> Result<T, ChangeBatchError> {
    if materialized {
        return build();
    }
    match tokio::task::spawn_blocking(build).await {
        Ok(parts) => parts,
        Err(join_err) => Err(ChangeBatchError::DeferredBuild {
            message: format!("deferred CDC batch build task failed: {join_err}"),
        }),
    }
}

/// [`ChangeEnvelope::into_parts_offloaded`] for a whole drained burst: one
/// blocking-pool handoff for the burst instead of one per envelope.
///
/// The decode work is unchanged; what is amortized is the `spawn_blocking` round
/// trip, so the win scales with envelopes-per-burst. A burst of
/// already-materialized envelopes resolves inline, with no handoff at all.
///
/// The first failed build discards the rest of the burst — callers MUST treat
/// the error as terminal for the dataset. The burst's committers are dropped
/// unacked, so the source re-streams from the last acked position.
pub async fn into_parts_offloaded_burst(
    envelopes: Vec<ChangeEnvelope>,
) -> Result<Vec<ChangeEnvelopeParts>, ChangeBatchError> {
    // `all` short-circuits, so a deferred source (the case this exists for) reads
    // exactly one envelope; only a fully-eager burst walks it.
    let materialized = envelopes.iter().all(ChangeEnvelope::is_materialized);
    offload_build(materialized, move || {
        envelopes
            .into_iter()
            .map(ChangeEnvelope::into_parts)
            .collect()
    })
    .await
}

/// A [`CommitChange`] implementation that does nothing. Useful when emitting
/// synthetic envelopes (e.g. ready signals) that have no underlying source
/// offset to commit.
pub struct NoOpCommitter;

#[async_trait]
impl CommitChange for NoOpCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }

    fn is_no_op(&self) -> bool {
        true
    }
}

/// Emit one uniform log line when a CDC committer durably acks source progress,
/// showing the source-commit timestamp of the data it commits and the
/// end-to-end lag (`now − source_commit_ts_ms`). Every connector's committer
/// calls this so `refresh_mode: changes` freshness and lag-based readiness can
/// be verified from the logs with a single filter (`spice_cdc::commit`).
///
/// `source_commit_ts_ms` is `None` for snapshot-boundary / no-timestamp commits
/// (lag is then reported as `None`).
/// Convert a [`SystemTime`] to milliseconds since the Unix epoch, or `None` if it
/// predates the epoch or overflows `i64`.
#[must_use]
pub fn system_time_to_unix_ms(t: SystemTime) -> Option<i64> {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
}

/// Current wall-clock time as milliseconds since the Unix epoch, or `None` if the
/// clock is unavailable.
#[must_use]
pub fn now_unix_ms() -> Option<i64> {
    system_time_to_unix_ms(SystemTime::now())
}

/// Replication lag in milliseconds: wall-clock now minus the source-commit
/// timestamp, clamped to `>= 0`. `None` when the source timestamp is unknown or
/// the clock is unavailable. Shared by the `spice_cdc::*` log lines and the lag
/// gauge so every CDC connector computes lag identically.
#[must_use]
pub fn replication_lag_ms(source_commit_ts_ms: Option<i64>) -> Option<i64> {
    match (now_unix_ms(), source_commit_ts_ms) {
        (Some(now), Some(ts)) => Some(now.saturating_sub(ts).max(0)),
        _ => None,
    }
}

pub fn log_committer_progress(
    connector: &str,
    dataset: &str,
    position: &str,
    source_commit_ts_ms: Option<i64>,
) {
    let lag_ms = replication_lag_ms(source_commit_ts_ms);
    tracing::debug!(
        target: "spice_cdc::commit",
        connector,
        dataset,
        position,
        source_commit_ts_ms = ?source_commit_ts_ms,
        lag_ms = ?lag_ms,
        "CDC committer acked source position"
    );
}

/// Construct a zero-row "heartbeat" [`ChangeEnvelope`] stamped with a
/// source-attested `source_commit_ts_ms` and carrying `is_dataset_ready`.
///
/// CDC connectors emit these to keep **lag-based readiness** live on an idle
/// source: a caught-up but quiet source has no rows to carry a freshness
/// timestamp, so without a heartbeat its measured lag would climb forever and
/// the dataset would never flip Ready. Periodically emitting a zero-row
/// envelope stamped with the source's own clock (a Postgres keepalive time, a
/// `MongoDB` cluster time, a `MySQL` server clock) lets the runtime observe
/// `now - source_commit_ts_ms` and mark the dataset Ready once that lag is
/// within the connector's `ready_lag`.
///
/// The batch has zero rows and a no-op committer — idle progress is
/// acknowledged through the connector's own keepalive/position handling, not
/// through this envelope's committer.
pub fn build_heartbeat_envelope(
    schema: &SchemaRef,
    source_commit_ts_ms: Option<i64>,
    is_dataset_ready: bool,
) -> Result<ChangeEnvelope, ChangeBatchError> {
    // Normalize fields to all-nullable so this empty barrier batch's struct type
    // matches the truncate/snapshot/live change batches it coalesces with. The
    // dataset schema may declare non-null columns (e.g. a `nullable: false`
    // primary key in the spicepod), but every other change batch uses the
    // nullable schema; without this, concat fails ("arrays of different data
    // types") when the heartbeat is coalesced with real data.
    let nullable_schema = Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect::<Vec<_>>(),
    );

    // Build zero-row versions of each dataset column.
    let empty_data_columns: Vec<ArrayRef> = nullable_schema
        .fields()
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();
    let data_struct = StructArray::new(nullable_schema.fields().clone(), empty_data_columns, None);

    let op_array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
    let pk_list = ListArray::new(
        Arc::clone(&pk_field),
        OffsetBuffer::new(vec![0i32].into()),
        Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        None,
    );

    let wrapper_schema = Arc::new(changes_schema(&nullable_schema));
    let record = RecordBatch::try_new(
        wrapper_schema,
        vec![op_array, Arc::new(pk_list), Arc::new(data_struct)],
    )
    .context(ArrowSnafu)?;
    let batch = ChangeBatch::try_new(record)?.with_source_commit_ts_ms(source_commit_ts_ms);

    Ok(ChangeEnvelope::new(
        Box::new(NoOpCommitter),
        batch,
        is_dataset_ready,
    ))
}

/// Construct an empty [`ChangeEnvelope`] whose only job is to flip
/// `is_dataset_ready=true`. The batch contains zero rows and uses a no-op
/// committer.
///
/// For sources with a binary caught-up-or-not readiness signal (e.g. Kafka
/// consumer lag reaching zero). Sources with a continuous freshness clock use
/// [`build_heartbeat_envelope`] with [`source_commit_within_ready_lag`] for
/// lag-based readiness instead. See the [`ChangesStream`] documentation for
/// the readiness contract.
pub fn build_ready_signal_envelope(schema: &SchemaRef) -> Result<ChangeEnvelope, ChangeBatchError> {
    build_heartbeat_envelope(schema, None, true)
}

/// Lag-based readiness predicate shared by CDC connectors: returns `true` when
/// `source_commit_ts_ms` is within `ready_lag` of now (wall clock). `None` (the
/// connector has no upstream timestamp yet) is **not** ready — there is no
/// freshness signal proving the stream has caught up. A source clock slightly
/// ahead of ours (small skew) clamps to zero lag and reads as ready.
///
/// This is the single definition of "caught up" behind every connector's
/// `{connector}_replication_ready_lag`: connectors stamp each envelope's
/// `is_dataset_ready` with it (mirroring `DynamoDB`'s poll-cycle lag gate), and a
/// [`build_heartbeat_envelope`] on an idle source carries the same verdict.
#[must_use]
pub fn source_commit_within_ready_lag(
    source_commit_ts_ms: Option<i64>,
    ready_lag: Duration,
) -> bool {
    let Some(lag_ms) = replication_lag_ms(source_commit_ts_ms) else {
        return false;
    };
    u128::from(lag_ms.unsigned_abs()) < ready_lag.as_millis()
}

/// The Arrow schema that represents a `ChangeEvent`
#[must_use]
pub fn changes_schema(table_schema: &Schema) -> Schema {
    Schema::new(vec![
        Field::new("op", DataType::Utf8, false),
        Field::new(
            "primary_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
        Field::new(
            "data",
            DataType::Struct(table_schema.fields().clone()),
            true,
        ),
    ])
}

#[derive(Clone, Debug)]
pub struct ChangeBatch {
    pub record: RecordBatch,
    op_idx: usize,
    primary_keys_idx: usize,
    data_idx: usize,
    /// Newest upstream COMMIT timestamp in this batch (milliseconds since the Unix
    /// epoch — a wall clock, NOT a monotonic `Instant`), when the source provides
    /// one; `None` otherwise. Lets a downstream consumer compute true end-to-end
    /// replication lag as `now_ms - source_commit_ts_ms`. Populated by CDC
    /// connectors that carry a source timestamp (Debezium, Postgres logical
    /// replication, `MongoDB` change streams); left `None` by sources that don't.
    source_commit_ts_ms: Option<i64>,
}

pub enum ChangeOperation {
    Create,
    Update,
    Delete,
    Read,
    Truncate,
    Unknown(String),
}

impl From<&str> for ChangeOperation {
    fn from(op: &str) -> Self {
        match op {
            "c" => Self::Create,
            "u" => Self::Update,
            "d" => Self::Delete,
            "r" => Self::Read,
            "t" => Self::Truncate,
            _ => Self::Unknown(op.to_string()),
        }
    }
}

impl Display for ChangeOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create => write!(f, "c"),
            Self::Update => write!(f, "u"),
            Self::Delete => write!(f, "d"),
            Self::Read => write!(f, "r"),
            Self::Truncate => write!(f, "t"),
            Self::Unknown(op) => write!(f, "Unknown({op})"),
        }
    }
}

impl ChangeBatch {
    pub fn try_new(record: RecordBatch) -> Result<Self, ChangeBatchError> {
        let schema = record.schema();
        Self::validate_schema(Arc::clone(&schema))?;

        let Some((op_idx, _)) = schema.column_with_name("op") else {
            unreachable!("The schema is validated to have an 'op' field")
        };
        let Some((primary_keys_idx, _)) = schema.column_with_name("primary_keys") else {
            unreachable!("The schema is validated to have a 'primary_keys' field")
        };
        let Some((data_idx, _)) = schema.column_with_name("data") else {
            unreachable!("The schema is validated to have a 'data' field")
        };

        Ok(Self {
            record,
            op_idx,
            primary_keys_idx,
            data_idx,
            source_commit_ts_ms: None,
        })
    }

    /// Attach the newest upstream commit timestamp (ms since the Unix epoch) for
    /// this batch. Connectors that carry a source timestamp set it here; the value
    /// rides the batch into the accelerator write path, where it feeds the
    /// replication-lag signal. `None` leaves the batch without lag information.
    #[must_use]
    pub fn with_source_commit_ts_ms(mut self, source_commit_ts_ms: Option<i64>) -> Self {
        self.source_commit_ts_ms = source_commit_ts_ms;
        self
    }

    /// The newest upstream commit timestamp (ms since the Unix epoch) in this
    /// batch, or `None` when the source does not provide one.
    #[must_use]
    pub fn source_commit_ts_ms(&self) -> Option<i64> {
        self.source_commit_ts_ms
    }

    /// Whether this is a zero-row envelope — a keepalive/heartbeat carrying only a
    /// `source_commit_ts_ms` to keep the idle lag gauge fresh, not an actual change.
    /// No source currently emits these (the Postgres heartbeat fan-out was reverted),
    /// so this is a defensive guard: consumers that derive *received/applied progress
    /// frontiers* must exclude such envelopes, because they are stamped with the server
    /// clock on a "keepalive ⇒ caught up" premise that is FALSE mid-backlog (keepalives
    /// interleave between transactions) — counting them advances the frontier past data
    /// not yet received/applied and corrupts the progress-rate ladder. It also correctly
    /// excludes empty-transaction envelopes from the data frontier.
    #[must_use]
    pub fn is_heartbeat(&self) -> bool {
        self.record.num_rows() == 0
    }

    #[must_use]
    pub fn op(&self, row: usize) -> ChangeOperation {
        let Some(op_col) = self
            .record
            .column(self.op_idx)
            .as_any()
            .downcast_ref::<StringArray>()
        else {
            unreachable!("The schema is validated to have an 'op' field which is a StringArray");
        };
        op_col.value(row).into()
    }

    #[must_use]
    pub fn primary_keys(&self, row: usize) -> Vec<String> {
        let Some(primary_keys_col) = self
            .record
            .column(self.primary_keys_idx)
            .as_any()
            .downcast_ref::<ListArray>()
        else {
            unreachable!(
                "The schema is validated to have a 'primary_keys' field which is a ListArray"
            );
        };
        let primary_keys_values = primary_keys_col.value(row);
        let Some(primary_keys_values) = primary_keys_values.as_any().downcast_ref::<StringArray>()
        else {
            unreachable!(
                "The schema is validated to have a 'primary_keys' field which is a ListArray of StringArray"
            );
        };
        let num_keys = primary_keys_values.len();
        let mut primary_keys: Vec<String> = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            primary_keys.push(primary_keys_values.value(i).to_string());
        }

        primary_keys
    }

    /// Whether `row` carries any primary key, without allocating the key list.
    ///
    /// [`Self::primary_keys`] materializes a `Vec<String>` (cloning every key)
    /// just to return the names; callers that only need to know whether a row is
    /// keyed (e.g. the CDC delete path partitioning keyed vs keyless rows) should
    /// use this — it reads the list length straight off the `ListArray` offsets.
    #[must_use]
    pub fn has_primary_keys(&self, row: usize) -> bool {
        let Some(primary_keys_col) = self
            .record
            .column(self.primary_keys_idx)
            .as_any()
            .downcast_ref::<ListArray>()
        else {
            unreachable!(
                "The schema is validated to have a 'primary_keys' field which is a ListArray"
            );
        };
        primary_keys_col.value_length(row) > 0
    }

    #[must_use]
    pub fn data(&self, row: usize) -> RecordBatch {
        let Some(data_col) = self
            .record
            .column(self.data_idx)
            .as_any()
            .downcast_ref::<StructArray>()
        else {
            unreachable!("The schema is validated to have a 'data' field which is a StructArray");
        };
        data_col.slice(row, 1).into()
    }

    #[must_use]
    pub fn data_batch(&self) -> RecordBatch {
        let data_col = self.record.column(self.data_idx);
        let Some(data_array) = data_col.as_any().downcast_ref::<StructArray>() else {
            unreachable!("The schema is validated to have a 'data' field which is a StructArray");
        };
        let DataType::Struct(fields) = data_array.data_type() else {
            unreachable!("The schema is validated to have a 'data' field which is a StructArray");
        };
        let Ok(record_batch) = RecordBatch::try_new(
            Arc::new(Schema::new(fields.clone())),
            data_array.columns().to_vec(),
        ) else {
            unreachable!("The schema is validated to have a 'data' field which is a StructArray");
        };
        record_batch
    }

    fn validate_schema(schema: SchemaRef) -> Result<(), ChangeBatchError> {
        let Some(data_col) = schema.fields().iter().find(|field| field.name() == "data") else {
            return SchemaMismatchSnafu {
                detail: "Missing 'data' field",
                schema,
            }
            .fail();
        };

        let data_schema = match data_col.data_type() {
            DataType::Struct(fields) => Schema::new(fields.clone()),
            _ => {
                return SchemaMismatchSnafu {
                    detail: "Unexpected data type for 'data' field, expected Struct",
                    schema,
                }
                .fail();
            }
        };

        let expected_schema = changes_schema(&data_schema);
        if *schema != expected_schema {
            return SchemaMismatchSnafu {
                detail: "Schema didn't match expected change batch format",
                schema,
            }
            .fail();
        }

        Ok(())
    }
}

/// Wraps an arbitrary data `RecordBatch` as a `ChangeBatch` with "create" operations.
pub fn wrap_data_as_change_batch(
    table_schema: &SchemaRef,
    data: &RecordBatch,
) -> Result<ChangeBatch, ChangeBatchError> {
    let num_rows = data.num_rows();
    let schema = changes_schema(table_schema);

    // 1) op column ("create" operations)
    let op_array = Arc::new(arrow::array::StringArray::from(vec![
        "c".to_string();
        num_rows
    ]));

    // 2) Dummy primary_keys: List<Utf8> with EMPTY LIST per row
    // Offsets must be length = num_rows + 1. All zeros => [] for every row.
    let offsets = vec![0i32; num_rows + 1];
    let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let primary_keys_array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(offsets.into()),
        values,
        None, // no validity bitmap (all non-null lists)
    ));

    // 3) data: Struct matching the input batch's schema/columns
    let data_array = Arc::new(StructArray::new(
        data.schema().fields().clone(),
        data.columns().to_vec(),
        None,
    ));

    let columns = vec![op_array, primary_keys_array, data_array];
    let record_batch = RecordBatch::try_new(schema.into(), columns).context(ArrowSnafu)?;

    ChangeBatch::try_new(record_batch)
}

pub fn replace_change_batch_data(
    new_data: &RecordBatch,
    change: &ChangeBatch,
) -> Result<ChangeBatch, ChangeBatchError> {
    let schema = changes_schema(&new_data.schema());

    let cols = change
        .record
        .schema()
        .fields()
        .iter()
        .map(|f| {
            if f.name() == "data" {
                Arc::new(StructArray::new(
                    new_data.schema().fields().clone(),
                    new_data.columns().to_vec(),
                    None,
                )) as Arc<dyn Array>
            } else {
                match change.record.column_by_name(f.name()) {
                    Some(column) => Arc::clone(column),
                    None => unreachable!("Column {} must exist", f.name()),
                }
            }
        })
        .collect();

    RecordBatch::try_new(schema.into(), cols)
        .map_err(|source| ChangeBatchError::Arrow { source })
        .and_then(ChangeBatch::try_new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn noop_committer_does_not_support_deferral() {
        // The conservative default: a committer with no replayable source offset
        // (`NoOpCommitter` carries synthetic ready-signal envelopes) must NOT be
        // deferred — deferring it advances nothing and there is nothing to
        // re-stream, so an in-memory durability tier must never arm on it.
        assert!(!NoOpCommitter.supports_deferral());
    }

    #[test]
    fn test_wrap_batch_as_change_batch() {
        // Create a test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create test data
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));
        let data_batch = RecordBatch::try_new(Arc::clone(&schema), vec![id_array, name_array])
            .expect("to create data batch");

        let change_batch =
            wrap_data_as_change_batch(&schema, &data_batch).expect("to create change batch");

        let record = &change_batch.record;

        // Verify the schema has the expected fields
        assert_eq!(record.schema().fields().len(), 3);
        // Verify the number of rows
        assert_eq!(record.num_rows(), 3);

        // Verify the op column
        let op_column = record
            .column_by_name("op")
            .expect("op column exists")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("op column is StringArray");
        for i in 0..3 {
            assert_eq!(op_column.value(i), "c");
        }

        // Verify the primary_keys column (should be empty lists)
        let pk_column = record
            .column_by_name("primary_keys")
            .expect("primary_keys column exists")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("primary_keys column is ListArray");
        assert_eq!(pk_column.len(), 3);
        for i in 0..3 {
            assert_eq!(pk_column.value_length(i), 0);
        }

        // Verify the data column
        let data_column = record
            .column_by_name("data")
            .expect("data column exists")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("data column is StructArray");
        assert_eq!(data_column.len(), 3);
        assert_eq!(data_column.num_columns(), 2);
    }
}

#[cfg(test)]
mod deferred_tests {
    //! Behavior of a deferred (destination-passing) [`ChangeEnvelope`]: metadata
    //! is answered without building, the build runs at most once on first
    //! access, and a build failure surfaces as a typed error (never a dropped or
    //! empty batch) that converts to a `StreamError` for the dataset's stream.
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::Int32Array;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn sample_batch(rows: i32) -> ChangeBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from((0..rows).collect::<Vec<_>>()))],
        )
        .expect("data batch");
        wrap_data_as_change_batch(&schema, &data).expect("wrap change batch")
    }

    /// A [`ChangeRows`] source whose build we can observe (count) and control
    /// (succeed with a batch, or fail).
    struct MockRows {
        result: Option<ChangeBatch>,
        builds: Arc<AtomicUsize>,
        rows_hint: usize,
        empty: bool,
        ts: Option<i64>,
    }

    impl ChangeRows for MockRows {
        fn is_empty(&self) -> bool {
            self.empty
        }
        fn num_rows_hint(&self) -> usize {
            self.rows_hint
        }
        fn encoded_len(&self) -> usize {
            0
        }
        fn source_commit_ts_ms(&self) -> Option<i64> {
            self.ts
        }
        fn is_heartbeat(&self) -> bool {
            false
        }
        fn build(self: Box<Self>) -> Result<ChangeBatch, ChangeBatchError> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            self.result.ok_or(ChangeBatchError::DeferredBuild {
                message: "mock build failure".to_string(),
            })
        }
    }

    fn deferred(rows: MockRows, ready: bool) -> ChangeEnvelope {
        ChangeEnvelope::new_from_rows(Box::new(NoOpCommitter), Box::new(rows), ready)
    }

    #[test]
    fn metadata_answered_without_building() {
        let builds = Arc::new(AtomicUsize::new(0));
        let env = deferred(
            MockRows {
                result: Some(sample_batch(3)),
                builds: Arc::clone(&builds),
                rows_hint: 3,
                empty: false,
                ts: Some(42),
            },
            false,
        );
        assert!(!env.is_empty());
        assert_eq!(env.num_rows_hint(), 3);
        assert_eq!(env.source_commit_ts_ms(), Some(42));
        assert!(!env.is_heartbeat());
        assert_eq!(
            builds.load(Ordering::SeqCst),
            0,
            "no-build metadata must not trigger the deferred build"
        );
    }

    #[test]
    fn change_batch_builds_once_and_caches() {
        let builds = Arc::new(AtomicUsize::new(0));
        let env = deferred(
            MockRows {
                result: Some(sample_batch(2)),
                builds: Arc::clone(&builds),
                rows_hint: 2,
                empty: false,
                ts: None,
            },
            false,
        );
        assert_eq!(env.change_batch().expect("build ok").record.num_rows(), 2);
        // Second access returns the cached batch without rebuilding.
        assert_eq!(env.change_batch().expect("cached").record.num_rows(), 2);
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "deferred build runs exactly once"
        );
    }

    #[test]
    fn into_parts_builds_deferred_batch() {
        let builds = Arc::new(AtomicUsize::new(0));
        let env = deferred(
            MockRows {
                result: Some(sample_batch(1)),
                builds: Arc::clone(&builds),
                rows_hint: 1,
                empty: false,
                ts: None,
            },
            true,
        );
        let (_committer, batch, ready) = env.into_parts().expect("into_parts builds ok");
        assert_eq!(batch.record.num_rows(), 1);
        assert!(ready);
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn deferred_build_failure_surfaces_as_typed_error() {
        let builds = Arc::new(AtomicUsize::new(0));
        let env = deferred(
            MockRows {
                result: None, // build fails
                builds: Arc::clone(&builds),
                rows_hint: 1,
                empty: false,
                ts: None,
            },
            false,
        );
        let err = env.change_batch().expect_err("build should fail");
        assert!(
            matches!(err, ChangeBatchError::DeferredBuild { .. }),
            "build failure must be a typed ChangeBatchError, got {err:?}"
        );
        // The consumer converts it to a stream error surfaced on the dataset's
        // stream — the batch is never silently dropped or applied empty.
        let stream_err: StreamError = err.into();
        assert!(matches!(stream_err, StreamError::External(_)));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn eager_envelope_is_ready_and_reports_exact_metadata() {
        // The blanket `ChangeRows for ChangeBatch` path: an eagerly-built
        // envelope reports exact metadata and needs no deferred build.
        let env = ChangeEnvelope::new(Box::new(NoOpCommitter), sample_batch(0), true);
        assert!(env.is_empty(), "zero-row batch is empty");
        assert_eq!(env.num_rows_hint(), 0);
        assert!(env.is_dataset_ready());
        assert_eq!(
            env.change_batch().expect("already built").record.num_rows(),
            0
        );
    }

    // ----- burst-wide deferred build -----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn burst_builds_every_deferred_envelope_in_order() {
        let builds = Arc::new(AtomicUsize::new(0));
        let envelopes: Vec<ChangeEnvelope> = [1i32, 2, 3]
            .into_iter()
            .map(|rows| {
                deferred(
                    MockRows {
                        result: Some(sample_batch(rows)),
                        builds: Arc::clone(&builds),
                        rows_hint: usize::try_from(rows).expect("positive row count"),
                        empty: false,
                        ts: None,
                    },
                    false,
                )
            })
            .collect();

        let parts = into_parts_offloaded_burst(envelopes)
            .await
            .expect("burst builds ok");

        assert_eq!(
            parts
                .iter()
                .map(|(_, batch, _)| batch.record.num_rows())
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "burst order must be preserved — committers pair with their batches"
        );
        assert_eq!(builds.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn burst_of_materialized_envelopes_resolves_inline() {
        // Eager sources materialize on delivery, so the whole burst must resolve
        // through the no-handoff path and still return every part in order.
        let envelopes = vec![
            ChangeEnvelope::new(Box::new(NoOpCommitter), sample_batch(2), false),
            ChangeEnvelope::new(Box::new(NoOpCommitter), sample_batch(4), true),
        ];
        assert!(envelopes.iter().all(ChangeEnvelope::is_materialized));

        let parts = into_parts_offloaded_burst(envelopes)
            .await
            .expect("materialized burst resolves");

        assert_eq!(
            parts
                .iter()
                .map(|(_, batch, ready)| (batch.record.num_rows(), *ready))
                .collect::<Vec<_>>(),
            vec![(2, false), (4, true)]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn burst_surfaces_a_failed_build_as_a_typed_error() {
        let builds = Arc::new(AtomicUsize::new(0));
        let envelopes = vec![
            deferred(
                MockRows {
                    result: Some(sample_batch(1)),
                    builds: Arc::clone(&builds),
                    rows_hint: 1,
                    empty: false,
                    ts: None,
                },
                false,
            ),
            deferred(
                MockRows {
                    result: None, // build fails
                    builds: Arc::clone(&builds),
                    rows_hint: 1,
                    empty: false,
                    ts: None,
                },
                false,
            ),
        ];

        // `expect_err` is unavailable: the Ok side holds a `Box<dyn CommitChange>`,
        // which is not `Debug`.
        match into_parts_offloaded_burst(envelopes).await {
            Ok(_) => panic!("a failed build must fail the burst"),
            Err(err) => assert!(
                matches!(err, ChangeBatchError::DeferredBuild { .. }),
                "expected a typed DeferredBuild error, got {err:?}"
            ),
        }
    }

    // ----- lag-based readiness helpers -----

    #[test]
    fn source_commit_within_ready_lag_gates_on_freshness() {
        let now = now_unix_ms().expect("clock available");
        let lag = Duration::from_secs(2);

        // A commit 500ms in the past is within a 2s window -> caught up.
        assert!(source_commit_within_ready_lag(Some(now - 500), lag));
        // A commit 5s in the past is beyond the window -> still behind.
        assert!(!source_commit_within_ready_lag(Some(now - 5_000), lag));
        // No upstream timestamp is never ready: there is no freshness proof.
        assert!(!source_commit_within_ready_lag(None, lag));
        // A source clock slightly ahead of ours (small skew) clamps to zero lag
        // and reads as ready rather than flapping.
        assert!(source_commit_within_ready_lag(Some(now + 500), lag));
    }

    #[test]
    fn source_commit_within_ready_lag_is_strict_at_the_boundary() {
        // `ready_lag` of zero can never be satisfied (lag is always >= 0 and the
        // comparison is strict `<`), so a zero threshold never marks Ready.
        let now = now_unix_ms().expect("clock available");
        assert!(!source_commit_within_ready_lag(
            Some(now),
            Duration::from_secs(0)
        ));
    }

    #[test]
    fn replication_lag_ms_clamps_and_handles_missing_ts() {
        // No source timestamp -> no lag signal.
        assert_eq!(replication_lag_ms(None), None);

        let now = now_unix_ms().expect("clock available");
        // A past commit reports a non-negative lag in the expected ballpark.
        let lag = replication_lag_ms(Some(now - 1_000)).expect("some lag");
        assert!((900..=60_000).contains(&lag), "unexpected lag: {lag}");

        // A future commit (source clock ahead) clamps to zero, never negative.
        assert_eq!(replication_lag_ms(Some(now + 10_000)), Some(0));
    }

    #[test]
    fn heartbeat_envelope_is_empty_stamps_ts_and_propagates_ready_flag() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let ts = now_unix_ms().expect("clock available");

        for ready in [true, false] {
            let env = build_heartbeat_envelope(&schema, Some(ts), ready)
                .expect("heartbeat envelope builds");
            assert_eq!(env.is_dataset_ready(), ready);
            let batch = env.change_batch().expect("already built");
            assert_eq!(batch.record.num_rows(), 0, "heartbeat carries no rows");
            assert!(
                batch.is_heartbeat(),
                "zero-row stamped batch is a heartbeat"
            );
            assert_eq!(
                batch.source_commit_ts_ms(),
                Some(ts),
                "heartbeat carries the source-attested clock"
            );
        }
    }

    #[test]
    fn heartbeat_envelope_builds_for_non_null_primary_key_schema() {
        // A `nullable: false` column must not break the coalesce with real
        // change batches: the heartbeat normalizes fields to nullable.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let env = build_heartbeat_envelope(&schema, None, false)
            .expect("heartbeat builds with non-null columns");
        assert_eq!(env.change_batch().expect("built").record.num_rows(), 0);
    }

    /// A committer whose `commit` happens to do nothing here but does NOT
    /// override `is_no_op` — modeling a real committer (resume-token persist,
    /// snapshot-boundary ack) attached to a zero-row envelope.
    struct PositionCommitter;

    #[async_trait]
    impl CommitChange for PositionCommitter {
        async fn commit(&self) -> Result<(), CommitError> {
            Ok(())
        }
    }

    #[test]
    fn no_op_heartbeat_predicate_identifies_strippable_readiness_envelopes() {
        assert!(NoOpCommitter.is_no_op());
        assert!(
            !PositionCommitter.is_no_op(),
            "is_no_op must default to false: assume a commit has effects"
        );

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // Connector idle heartbeats and ready signals: zero rows + no-op
        // committer -> safe to drop from the write/durability path (#12007).
        let heartbeat = build_heartbeat_envelope(&schema, now_unix_ms(), true)
            .expect("heartbeat envelope builds");
        assert!(heartbeat.is_no_op_heartbeat());
        let ready = build_ready_signal_envelope(&schema).expect("ready envelope builds");
        assert!(ready.is_no_op_heartbeat());

        // A zero-row envelope re-wrapped with a REAL committer (the MySQL
        // snapshot-boundary pattern) must NOT be treated as a heartbeat: its
        // commit persists source progress and needs durability-then-commit
        // ordering.
        let (_, boundary_batch, _) = build_heartbeat_envelope(&schema, None, false)
            .expect("boundary batch builds")
            .into_parts()
            .expect("already built");
        let boundary = ChangeEnvelope::new(Box::new(PositionCommitter), boundary_batch, false);
        assert!(boundary.is_heartbeat(), "zero-row batch");
        assert!(
            !boundary.is_no_op_heartbeat(),
            "a real committer disqualifies the envelope from heartbeat stripping"
        );

        // A row-bearing (deferred, not yet built) envelope with a no-op
        // committer is not a heartbeat either.
        let rows = MockRows {
            result: None,
            builds: Arc::new(AtomicUsize::new(0)),
            rows_hint: 1,
            empty: false,
            ts: None,
        };
        let data_bearing = deferred(rows, false);
        assert!(!data_bearing.is_no_op_heartbeat());
    }
}
