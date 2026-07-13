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

//! `MySQL` binlog replication for Spice acceleration.
//!
//! Streams row-based binlog changes from a source `MySQL` database directly
//! into the local accelerator via the existing [`crate::cdc::ChangesStream`]
//! abstraction — the `MySQL` analog of [`crate::postgres_replication`].
//!
//! The structural difference from Postgres: `MySQL` keeps no server-side
//! per-replica cursor (there is no replication-slot equivalent), so the
//! stream's ack is a client-persisted [`config::BinlogPosition`] stored via
//! the caller-supplied [`PositionStore`] (the runtime persists it in the
//! accelerator's `spice_sys_mysql_binlog` sidecar table).
//!
//! Entry point: [`start_replication_stream`]. Compiled as part of the
//! `mysql` feature on this crate — no separate feature flag.

pub mod binlog;
pub mod bootstrap;
pub mod config;
pub mod metrics;
pub mod resilience;
pub mod rows;
pub mod setup;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use snafu::Snafu;

use crate::cdc::{
    ChangeEnvelope, ChangesStream, CommitChange, CommitError, NoOpCommitter, StreamError,
    build_heartbeat_envelope,
};

use crate::cdc::{InitialSnapshotMode, InvalidCheckpointBehavior};
pub use config::{BinlogPosition, ReplicationParams, derive_server_id, process_nonce};
pub use metrics::{Metrics as ReplicationMetrics, MetricsCollector as ReplicationMetricsCollector};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to MySQL: {source}"))]
    Connect { source: mysql_async::Error },

    #[snafu(display("Failed to execute setup query ({context}): {source}"))]
    SetupQuery {
        context: String,
        source: mysql_async::Error,
    },

    #[snafu(display(
        "Binary logging is not enabled on this MySQL server (`log_bin = OFF`). \
         Start the server with binary logging enabled (`--log-bin`); it is on by \
         default on MySQL 8.0+."
    ))]
    BinaryLoggingDisabled,

    #[snafu(display(
        "MySQL `binlog_format` is '{format}', but row-based replication requires 'ROW'. \
         Run: SET GLOBAL binlog_format = 'ROW'; (and update my.cnf so it persists). \
         Sessions opened before the change keep their old format until they reconnect."
    ))]
    UnsupportedBinlogFormat { format: String },

    #[snafu(display(
        "MySQL `binlog_row_image` is '{image}', but Spice requires 'FULL' so every \
         change event carries complete row images. Run: SET GLOBAL binlog_row_image = 'FULL'; \
         (and update my.cnf so it persists)."
    ))]
    UnsupportedBinlogRowImage { image: String },

    #[snafu(display(
        "MySQL `binlog_row_value_options` is '{options}', but Spice requires it to be \
         empty: partial JSON row images cannot be applied. \
         Run: SET GLOBAL binlog_row_value_options = ''; (and update my.cnf so it persists)."
    ))]
    UnsupportedBinlogRowValueOptions { options: String },

    #[snafu(display("Source table {database}.{table} does not exist"))]
    SourceTableNotFound { database: String, table: String },

    #[snafu(display(
        "Column `{column}` from the dataset schema does not exist on source table \
         {database}.{table}. The source schema changed — update the dataset schema \
         (or its `columns:` definition) to match."
    ))]
    ColumnMissing {
        column: String,
        database: String,
        table: String,
    },

    #[snafu(display("binlog decode error: {message}"))]
    Decode { message: String },

    #[snafu(display("Schema mismatch: {message}"))]
    SchemaMismatch { message: String },

    #[snafu(display("Failed to build the ready-signal change batch: {message}"))]
    BuildReadySignal { message: String },

    #[snafu(display("Bootstrap snapshot error: {message}"))]
    Bootstrap { message: String },

    #[snafu(display("Failed to access the persisted binlog position: {message}"))]
    PositionStoreAccess { message: String },

    #[snafu(display("{message}"))]
    StalePosition { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for [`PositionStore`] implementations.
pub type StoreError = Box<dyn std::error::Error + Send + Sync>;

/// A persisted replication checkpoint: the binlog position to resume from,
/// plus optional schema/layout snapshots for drift detection.
///
/// `schema_json` historically held only the dataset Arrow schema. That is
/// insufficient: binlog row images are positional, so a source-only reorder
/// or same-count reshape that leaves the dataset schema unchanged would
/// silently mis-map columns on resume. New checkpoints store a versioned
/// envelope in `schema_json` that also fingerprints the source ordinal
/// layout (see [`CheckpointMeta`]).
#[derive(Clone, Debug)]
pub struct PersistedPosition {
    pub position: BinlogPosition,
    pub schema_json: Option<String>,
}

/// Version tag for [`CheckpointMeta`] serialized into `schema_json`.
pub const CHECKPOINT_META_VERSION: u32 = 2;

/// Versioned checkpoint sidecar payload stored in `schema_json`.
///
/// v1 (legacy): the raw Arrow schema JSON object.
/// v2: this envelope, with both the dataset schema and a source-layout
/// fingerprint. Resume refuses to continue when either diverges.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CheckpointMeta {
    pub version: u32,
    /// Serialized dataset Arrow schema (same bytes `MySqlBinlogSys::serialize_schema` produces).
    pub dataset_schema_json: String,
    /// [`setup::TableLayout::fingerprint`] of the source table at checkpoint time.
    pub source_layout_fingerprint: String,
}

impl CheckpointMeta {
    #[must_use]
    pub fn new(dataset_schema_json: String, source_layout_fingerprint: String) -> Self {
        Self {
            version: CHECKPOINT_META_VERSION,
            dataset_schema_json,
            source_layout_fingerprint,
        }
    }

    /// Serialize for the `schema_json` sidecar column.
    pub fn to_schema_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Parse a stored `schema_json` value.
    ///
    /// - v2 envelope → `Ok(Some(meta))`
    /// - legacy raw Arrow schema JSON (object with a top-level `fields` key,
    ///   no usable `version`) → `Ok(None)` (caller must treat as unknown
    ///   layout and refuse unsafe resume)
    /// - corrupt / empty / non-schema JSON → `Err`
    pub fn parse(schema_json: &str) -> Result<Option<Self>, String> {
        let value: serde_json::Value =
            serde_json::from_str(schema_json).map_err(|e| e.to_string())?;
        if value
            .get("version")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|v| v >= u64::from(CHECKPOINT_META_VERSION))
        {
            let meta: Self = serde_json::from_value(value).map_err(|e| e.to_string())?;
            if meta.version != CHECKPOINT_META_VERSION {
                return Err(format!(
                    "unsupported mysql binlog checkpoint meta version {}",
                    meta.version
                ));
            }
            return Ok(Some(meta));
        }
        // Legacy: a bare Arrow schema JSON object (has `fields`, no usable
        // `version`). Anything else is corrupt / unsupported meta.
        if value.is_object() && value.get("fields").is_some() {
            return Ok(None);
        }
        Err(
            "persisted checkpoint schema_json is neither a v2 CheckpointMeta envelope nor a legacy Arrow schema object"
                .to_string(),
        )
    }
}

/// Build the `schema_json` value persisted with each checkpoint.
///
/// When the connector could not serialize the dataset schema, returns `None`
/// (resume will refuse rather than decode against an unverified layout).
#[must_use]
pub fn encode_checkpoint_schema_json(
    dataset_schema_json: Option<&str>,
    source_layout: &setup::TableLayout,
) -> Option<String> {
    let dataset_schema_json = dataset_schema_json?;
    match CheckpointMeta::new(dataset_schema_json.to_string(), source_layout.fingerprint())
        .to_schema_json()
    {
        Ok(json) => Some(json),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "failed to serialize mysql binlog checkpoint meta; resume will refuse rather than decode against an unverified layout"
            );
            None
        }
    }
}

/// Why a persisted checkpoint cannot safely resume against the current
/// source layout / dataset schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResumeDrift {
    /// Checkpoint predates layout fingerprinting (legacy raw Arrow schema).
    LegacyCheckpoint,
    /// Checkpoint has missing or invalid schema/layout metadata (absent,
    /// corrupt, or an unsupported meta version).
    MissingCheckpointMeta,
    /// Current run could not serialize the dataset schema, so resume has no
    /// comparable baseline against the persisted checkpoint.
    CurrentDatasetSchemaUnavailable,
    /// Dataset Arrow schema changed between runs.
    DatasetSchemaChanged,
    /// Source ordinal layout (names/types/order/PK) changed between runs.
    SourceLayoutChanged,
}

impl std::fmt::Display for ResumeDrift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LegacyCheckpoint => write!(
                f,
                "persisted checkpoint predates source-layout fingerprinting"
            ),
            Self::MissingCheckpointMeta => {
                write!(
                    f,
                    "persisted checkpoint has missing or invalid schema/layout metadata"
                )
            }
            Self::CurrentDatasetSchemaUnavailable => write!(
                f,
                "current dataset schema could not be serialized for resume compatibility"
            ),
            Self::DatasetSchemaChanged => {
                write!(f, "dataset schema changed since the checkpoint was written")
            }
            Self::SourceLayoutChanged => write!(
                f,
                "source table ordinal layout changed since the checkpoint was written"
            ),
        }
    }
}

/// Compare a persisted checkpoint against the current dataset schema JSON and
/// source layout fingerprint.
///
/// Returns `Ok(())` only when resume is safe: the checkpoint carries a v2
/// meta envelope whose dataset schema and source-layout fingerprint both
/// match. Any other outcome is [`ResumeDrift`] — the caller must error or
/// rebootstrap rather than decode historical row images with the current
/// layout (which would silently scramble columns on a reorder/retype).
pub fn check_resume_compatibility(
    persisted_schema_json: Option<&str>,
    current_dataset_schema_json: Option<&str>,
    current_layout_fingerprint: &str,
) -> Result<(), ResumeDrift> {
    let Some(stored) = persisted_schema_json else {
        return Err(ResumeDrift::MissingCheckpointMeta);
    };
    let meta = match CheckpointMeta::parse(stored) {
        Ok(Some(meta)) => meta,
        Ok(None) => return Err(ResumeDrift::LegacyCheckpoint),
        Err(_) => return Err(ResumeDrift::MissingCheckpointMeta),
    };
    match current_dataset_schema_json {
        Some(current) if dataset_schemas_equivalent(current, &meta.dataset_schema_json) => {}
        Some(_) => return Err(ResumeDrift::DatasetSchemaChanged),
        // Current run could not serialize the dataset schema — refuse rather
        // than resume without a comparable baseline.
        None => return Err(ResumeDrift::CurrentDatasetSchemaUnavailable),
    }
    if meta.source_layout_fingerprint != current_layout_fingerprint {
        return Err(ResumeDrift::SourceLayoutChanged);
    }
    Ok(())
}

/// Compare two Arrow schema JSON strings for equivalence.
///
/// Exact string match is preferred (fast path). Otherwise both sides are
/// parsed as [`serde_json::Value`] so map key order differences in field
/// metadata do not false-positive into [`ResumeDrift::DatasetSchemaChanged`].
fn dataset_schemas_equivalent(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    match (
        serde_json::from_str::<serde_json::Value>(a),
        serde_json::from_str::<serde_json::Value>(b),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

/// Durable storage for the dataset's binlog position — the client-side
/// replacement for a Postgres replication slot's server-tracked cursor.
///
/// The runtime implements this over the accelerator sidecar
/// (`spice_sys_mysql_binlog`); non-persistent accelerators use
/// [`NoopPositionStore`] and re-bootstrap on every start.
#[async_trait]
pub trait PositionStore: Send + Sync {
    async fn load(&self) -> std::result::Result<Option<PersistedPosition>, StoreError>;
    async fn save(&self, position: &PersistedPosition) -> std::result::Result<(), StoreError>;
    async fn clear(&self) -> std::result::Result<(), StoreError>;
}

/// A [`PositionStore`] that persists nothing. Used when the accelerator
/// doesn't survive restarts — there is no state a resumed position could be
/// consistent with, so every start re-bootstraps.
pub struct NoopPositionStore;

#[async_trait]
impl PositionStore for NoopPositionStore {
    async fn load(&self) -> std::result::Result<Option<PersistedPosition>, StoreError> {
        Ok(None)
    }
    async fn save(&self, _position: &PersistedPosition) -> std::result::Result<(), StoreError> {
        Ok(())
    }
    async fn clear(&self) -> std::result::Result<(), StoreError> {
        Ok(())
    }
}

/// Input required to start a replication stream for a single dataset.
pub struct ReplicationStreamInput {
    /// Dataset name for logs/errors.
    pub dataset_name: String,
    /// Parameters parsed from the connector's component-level params.
    pub params: ReplicationParams,
    /// Arrow schema of the dataset, used to shape `ChangeBatch` data columns.
    pub schema: SchemaRef,
    /// Primary-key column names as declared on the dataset's acceleration
    /// config.
    pub primary_keys: Vec<String>,
    /// Source database (schema) and table being replicated.
    pub database: String,
    pub table: String,
    /// Durable storage for the binlog position.
    pub position_store: Arc<dyn PositionStore>,
    /// Serialized snapshot of the dataset's Arrow schema, persisted with
    /// each checkpoint for drift detection on resume. The connector computes
    /// it (this crate's Arrow build doesn't enable serde); `None` disables
    /// drift detection.
    pub schema_json: Option<String>,
    /// Shared collector the stream updates as it processes events; the
    /// connector reads it via its `MetricsProvider`.
    pub metrics: Arc<ReplicationMetricsCollector>,
}

/// Starts the bootstrap+binlog replication stream.
///
/// The returned stream yields change envelopes the same way the Postgres
/// replication path does, so the rest of the refresh loop needs no changes.
///
/// Steps performed lazily on first poll:
///   1. Validate the server (`log_bin`, `binlog_format=ROW`,
///      `binlog_row_image=FULL`) and fetch the table's positional column
///      layout from `information_schema`.
///   2. Load the persisted binlog position. If one exists (and its file is
///      still on the server), resume streaming from it. Otherwise capture
///      the current binlog head, snapshot the table over a
///      `CONSISTENT SNAPSHOT` transaction, persist the head position, and
///      stream from there.
///   3. Hand off to the binlog dump stream ([`binlog`]).
///
/// The head position is captured *before* the snapshot begins, so changes
/// racing the snapshot are delivered at least once and converge via the
/// accelerator's PK upsert — the same contract as the Postgres
/// snapshot/WAL boundary.
#[must_use]
pub fn start_replication_stream(input: ReplicationStreamInput) -> ChangesStream {
    Box::pin(
        stream::once(async move { start_inner(input).await }).flat_map(|result| match result {
            Ok(stream) => stream,
            Err(e) => stream::once(async move { Err(stream_error(&e)) }).boxed(),
        }),
    )
}

async fn start_inner(input: ReplicationStreamInput) -> Result<ChangesStream> {
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

    // 1. Validate the server + discover the positional table layout.
    let mut conn = setup::connect(&params).await?;
    setup::validate_server(&mut conn).await?;
    let layout = setup::fetch_table_layout(&mut conn, &database, &table).await?;
    let column_map = layout.column_map(&schema, &database, &table)?;

    // Every declared PK must exist on the source; warn when it diverges from
    // the source PRIMARY KEY (legal — full row images let any column route
    // deletes — but usually a misconfiguration).
    for pk in &primary_keys {
        if !layout.columns.iter().any(|c| c.name == *pk) {
            return SchemaMismatchSnafu {
                message: format!(
                    "declared primary_key `{pk}` not found on source table {database}.{table}"
                ),
            }
            .fail();
        }
    }
    let source_pks = layout.primary_key_columns();
    if !source_pks.is_empty()
        && primary_keys.iter().map(String::as_str).collect::<Vec<_>>() != source_pks
    {
        tracing::warn!(
            dataset = %dataset_name,
            declared = ?primary_keys,
            source = ?source_pks,
            "dataset primary_key differs from the source table's PRIMARY KEY; \
             UPDATE/DELETE events are routed by the declared key"
        );
    }

    // 2. Load the persisted position and pick the start path.
    //
    // Binlog row images are positional. Resuming against a different source
    // ordinal layout (or dataset schema) would decode historical events with
    // the *current* name→index map and silently scramble columns whenever
    // types still convert. Refuse that case — error or rebootstrap per
    // `invalid_position_behavior`.
    let persisted = position_store
        .load()
        .await
        .map_err(|e| Error::PositionStoreAccess {
            message: e.to_string(),
        })?;

    let layout_fingerprint = layout.fingerprint();
    let checkpoint_schema_json = encode_checkpoint_schema_json(schema_json.as_deref(), &layout);

    let resume_position = match persisted {
        Some(persisted) if params.snapshot_mode == InitialSnapshotMode::Always => {
            tracing::info!(
                dataset = %dataset_name,
                position = %persisted.position,
                "`snapshot_mode: always`: running the initial snapshot despite a persisted \
                 binlog position"
            );
            None
        }
        Some(persisted) => {
            if let Err(drift) = check_resume_compatibility(
                persisted.schema_json.as_deref(),
                schema_json.as_deref(),
                &layout_fingerprint,
            ) {
                match params.invalid_position_behavior {
                    InvalidCheckpointBehavior::Error => {
                        return StalePositionSnafu {
                            message: format!(
                                "cannot resume mysql binlog for {dataset_name} from {}: {drift}. Replaying historical row images against the current source layout would mis-map columns. Set `mysql_replication_invalid_checkpoint_behavior: restart` to drop the saved position and re-snapshot the table.",
                                persisted.position
                            ),
                        }
                        .fail();
                    }
                    InvalidCheckpointBehavior::Restart => {
                        tracing::warn!(
                            dataset = %dataset_name,
                            position = %persisted.position,
                            drift = %drift,
                            "persisted binlog checkpoint is incompatible with the current source layout / dataset schema; rebootstrap behavior enabled, falling back to a fresh snapshot"
                        );
                        if let Err(e) = position_store.clear().await {
                            tracing::warn!(
                                dataset = %dataset_name,
                                error = %e,
                                "failed to clear the incompatible binlog position; the subsequent bootstrap will overwrite it"
                            );
                        }
                        None
                    }
                }
            } else if setup::binlog_file_exists(&mut conn, &persisted.position.file).await? {
                Some(persisted.position)
            } else {
                match params.invalid_position_behavior {
                    InvalidCheckpointBehavior::Error => {
                        return StalePositionSnafu {
                            message: format!(
                                "persisted binlog position {} is no longer on the server \
                                 (binary logs were purged). Set \
                                 `mysql_replication_invalid_checkpoint_behavior: restart` to \
                                 drop the saved position and re-snapshot the table, or increase \
                                 `binlog_expire_logs_seconds` on the source.",
                                persisted.position
                            ),
                        }
                        .fail();
                    }
                    InvalidCheckpointBehavior::Restart => {
                        tracing::warn!(
                            dataset = %dataset_name,
                            position = %persisted.position,
                            "persisted binlog position was purged from the source; restart \
                             behavior enabled, falling back to a fresh snapshot"
                        );
                        if let Err(e) = position_store.clear().await {
                            tracing::warn!(
                                dataset = %dataset_name,
                                error = %e,
                                "failed to clear the stale binlog position; the subsequent \
                                 bootstrap will overwrite it"
                            );
                        }
                        None
                    }
                }
            }
        }
        None => None,
    };

    // 3. Assemble the per-path prelude, then hand everything to one binlog
    //    stream:
    //      - resume:      ready signal; stream from the persisted position.
    //      - no snapshot: persist head + ready signal; stream from the head.
    //      - snapshot:    truncate barrier → snapshot rows → ready signal
    //                     carrying the head-position commit; stream from the
    //                     captured head.
    let (start, prelude): (BinlogPosition, ChangesStream) = if let Some(position) = resume_position
    {
        // Resume: no snapshot. Readiness is lag-based — the binlog stream marks
        // the dataset Ready once it has caught up to the source head (see
        // `binlog::start_binlog_stream` and `mysql_replication_ready_lag`), so a
        // quiet source whose first event is far away stays not-ready until a
        // heartbeat confirms it is caught up.
        if let Err(e) = conn.disconnect().await {
            tracing::debug!(dataset = %dataset_name, error = %e, "setup connection disconnect");
        }
        tracing::info!(
            dataset = %dataset_name,
            position = %position,
            "mysql replication: resuming binlog stream from persisted position; skipping snapshot"
        );
        metrics.mark_bootstrap_complete();
        (
            position,
            Box::pin(stream::empty::<
                std::result::Result<ChangeEnvelope, StreamError>,
            >()),
        )
    } else {
        // Cold start: capture the binlog head BEFORE any snapshot so the
        // overlap replays idempotently.
        let head = setup::fetch_head_position(&mut conn).await?;
        // Seed snapshot progress from the source's approximate row count
        // (`information_schema.TABLES`) so operators get a progress signal;
        // best-effort — absence just leaves the metric unset.
        if params.snapshot_mode != InitialSnapshotMode::Disabled {
            match setup::fetch_approx_row_count(&mut conn, &database, &table).await {
                Ok(Some(expected)) => metrics.set_bootstrap_rows_expected(expected),
                Ok(None) => {}
                Err(e) => {
                    tracing::debug!(dataset = %dataset_name, error = %e, "row-count estimate");
                }
            }
        }
        if let Err(e) = conn.disconnect().await {
            tracing::debug!(dataset = %dataset_name, error = %e, "setup connection disconnect");
        }

        if params.snapshot_mode == InitialSnapshotMode::Disabled {
            tracing::info!(
                dataset = %dataset_name,
                position = %head,
                "mysql replication: `initial_snapshot: disabled` — streaming changes from the \
                 current binlog head without snapshotting existing rows"
            );
            metrics.mark_bootstrap_complete();
            // Persist the start position up front: with no snapshot there is
            // no bootstrap barrier to piggy-back on, and resuming from `head`
            // after a restart is exactly the no-snapshot contract.
            let initial = PersistedPosition {
                position: head.clone(),
                schema_json: checkpoint_schema_json.clone(),
            };
            if let Err(e) = position_store.save(&initial).await {
                tracing::warn!(
                    dataset = %dataset_name,
                    error = %e,
                    "failed to persist the initial binlog position; a restart before the first \
                     checkpoint will re-attach at the then-current head"
                );
            }
            // Readiness is lag-based; the binlog stream marks the dataset Ready
            // once it has caught up to the head captured above.
            (
                head,
                Box::pin(stream::empty::<
                    std::result::Result<ChangeEnvelope, StreamError>,
                >()),
            )
        } else {
            // Lead with a TRUNCATE envelope so a re-bootstrap over a
            // persistent accelerator clears rows deleted on the source while
            // no position was held (no-op on an empty accelerator).
            let truncate = truncate_envelope(&schema, &primary_keys, &column_map)?;

            let snapshot = bootstrap::snapshot_stream(bootstrap::SnapshotInput {
                params: params.clone(),
                layout: layout.clone(),
                schema: Arc::clone(&schema),
                primary_keys: primary_keys.clone(),
                column_map: column_map.clone(),
                database: database.clone(),
                table: table.clone(),
                dataset_name: dataset_name.clone(),
                metrics: Arc::clone(&metrics),
            });

            // The captured head position is persisted by a zero-row
            // snapshot-boundary envelope's committer, after the runtime has
            // durably applied the whole snapshot. A crash before then leaves
            // the sidecar empty, so the next start re-bootstraps from scratch.
            // The envelope is NOT ready-signalling (`false`): readiness is
            // lag-based and comes from the binlog stream once it catches up to
            // `head`, so a large snapshot's replay backlog keeps the dataset
            // not-ready until it drains.
            let (_, boundary_batch, _) = build_heartbeat_envelope(&schema, None, false)
                .map_err(|e| Error::SchemaMismatch {
                    message: e.to_string(),
                })?
                .into_parts()
                .map_err(|e| Error::SchemaMismatch {
                    message: e.to_string(),
                })?;
            let boundary = ChangeEnvelope::from_parts(
                Box::new(InitialPositionCommitter {
                    store: Arc::clone(&position_store),
                    position: PersistedPosition {
                        position: head.clone(),
                        schema_json: checkpoint_schema_json.clone(),
                    },
                    dataset: dataset_name.clone(),
                }),
                boundary_batch,
                false,
            );

            let snapshot_prelude: ChangesStream = Box::pin(
                stream::once(async move { Ok(truncate) })
                    .chain(snapshot)
                    .chain(stream::once(async move { Ok(boundary) })),
            );
            (head, snapshot_prelude)
        }
    };

    let binlog = binlog::start_binlog_stream(binlog::BinlogStreamInput {
        params,
        layout,
        start,
        schema,
        primary_keys,
        column_map,
        database,
        table,
        dataset_name,
        position_store,
        // Persist the versioned checkpoint meta (dataset schema + source
        // layout fingerprint), not the bare Arrow schema — resume needs both.
        schema_json: checkpoint_schema_json,
        metrics,
    });

    Ok(Box::pin(prelude.chain(binlog)))
}

/// A single-row `op="t"` envelope with a no-op committer, emitted ahead of a
/// snapshot to clear stale accelerator state.
fn truncate_envelope(
    schema: &SchemaRef,
    primary_keys: &[String],
    column_map: &[usize],
) -> Result<ChangeEnvelope> {
    let batch =
        rows::build_change_batch(schema, primary_keys, column_map, &[rows::truncate_change()])?;
    Ok(ChangeEnvelope::new(Box::new(NoOpCommitter), batch, false))
}

/// Commits the bootstrap's captured head position to the sidecar. Runs after
/// the runtime has durably applied every snapshot envelope — the barrier
/// between the bootstrap and live phases.
struct InitialPositionCommitter {
    store: Arc<dyn PositionStore>,
    position: PersistedPosition,
    /// Dataset name, for the committer-progress log line.
    dataset: String,
}

#[async_trait]
impl CommitChange for InitialPositionCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.store
            .save(&self.position)
            .await
            .map_err(|source| CommitError::UnableToCommitChange { source })?;
        // Snapshot-boundary commit: no source-commit timestamp, so lag is `None`.
        crate::cdc::log_committer_progress(
            "mysql",
            &self.dataset,
            &self.position.position.to_string(),
            None,
        );
        Ok(())
    }
}

fn stream_error(err: &Error) -> StreamError {
    StreamError::External(err.to_string())
}

/// Helper for stream blocks to turn an [`Error`] into the `StreamError` the
/// cdc machinery speaks. Takes the error by value so it can be used directly
/// as a function pointer in `Result::map_err`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "used as a function pointer in map_err; taking by reference would require a closure at every call site"
)]
pub(crate) fn err_to_stream(err: Error) -> StreamError {
    stream_error(&err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql_replication::setup::{SourceColumn, TableLayout};

    fn layout(names_and_types: &[(&str, &str)]) -> TableLayout {
        TableLayout {
            columns: names_and_types
                .iter()
                .map(|(name, ty)| SourceColumn {
                    name: (*name).to_string(),
                    column_type: (*ty).to_string(),
                    enum_variants: None,
                    set_variants: None,
                    is_primary_key: false,
                })
                .collect(),
        }
    }

    #[test]
    fn checkpoint_meta_round_trips() {
        let meta = CheckpointMeta::new(
            r#"{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}"#.to_string(),
            layout(&[("id", "int")]).fingerprint(),
        );
        let json = meta.to_schema_json().expect("serialize");
        let parsed = CheckpointMeta::parse(&json)
            .expect("parse")
            .expect("v2 envelope");
        assert_eq!(parsed, meta);
    }

    #[test]
    fn checkpoint_meta_parse_legacy_arrow_schema_as_none() {
        // Pre-v2 checkpoints stored the raw Arrow schema JSON object.
        let legacy = r#"{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}"#;
        assert_eq!(
            CheckpointMeta::parse(legacy).expect("legacy parses"),
            None,
            "legacy Arrow schema JSON must be treated as unknown layout"
        );
    }

    #[test]
    fn checkpoint_meta_parse_non_schema_json_as_err() {
        assert!(
            CheckpointMeta::parse(r#""just a string""#).is_err(),
            "non-object JSON must not look like a legacy Arrow schema"
        );
        assert!(
            CheckpointMeta::parse(r#"{"not_fields":[]}"#).is_err(),
            "object without `fields` must not look like a legacy Arrow schema"
        );
        assert!(
            CheckpointMeta::parse("42").is_err(),
            "numeric JSON must not look like a legacy Arrow schema"
        );
    }

    #[test]
    fn resume_compatible_when_schema_and_layout_match() {
        let layout = layout(&[("id", "int"), ("name", "varchar(255)")]);
        let dataset = r#"{"fields":[]}"#;
        let meta = CheckpointMeta::new(dataset.to_string(), layout.fingerprint())
            .to_schema_json()
            .expect("serialize");
        check_resume_compatibility(Some(&meta), Some(dataset), &layout.fingerprint())
            .expect("matching checkpoint must resume");
    }

    #[test]
    fn resume_refuses_source_layout_reorder() {
        let old = layout(&[("id", "int"), ("name", "varchar(255)")]);
        let new = layout(&[("name", "varchar(255)"), ("id", "int")]);
        let dataset = r#"{"fields":[]}"#;
        let meta = CheckpointMeta::new(dataset.to_string(), old.fingerprint())
            .to_schema_json()
            .expect("serialize");
        assert_eq!(
            check_resume_compatibility(Some(&meta), Some(dataset), &new.fingerprint()),
            Err(ResumeDrift::SourceLayoutChanged)
        );
    }

    #[test]
    fn resume_refuses_dataset_schema_change() {
        let layout = layout(&[("id", "int")]);
        let meta = CheckpointMeta::new(r#"{"fields":["old"]}"#.to_string(), layout.fingerprint())
            .to_schema_json()
            .expect("serialize");
        assert_eq!(
            check_resume_compatibility(
                Some(&meta),
                Some(r#"{"fields":["new"]}"#),
                &layout.fingerprint()
            ),
            Err(ResumeDrift::DatasetSchemaChanged)
        );
    }

    #[test]
    fn resume_refuses_legacy_checkpoint() {
        let layout = layout(&[("id", "int")]);
        let legacy = r#"{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}"#;
        assert_eq!(
            check_resume_compatibility(Some(legacy), Some(legacy), &layout.fingerprint()),
            Err(ResumeDrift::LegacyCheckpoint)
        );
    }

    #[test]
    fn resume_refuses_missing_meta() {
        let layout = layout(&[("id", "int")]);
        assert_eq!(
            check_resume_compatibility(None, Some("{}"), &layout.fingerprint()),
            Err(ResumeDrift::MissingCheckpointMeta)
        );
    }

    #[test]
    fn resume_refuses_when_current_schema_unavailable() {
        let layout = layout(&[("id", "int")]);
        let meta = CheckpointMeta::new(r#"{"fields":[]}"#.to_string(), layout.fingerprint())
            .to_schema_json()
            .expect("serialize");
        assert_eq!(
            check_resume_compatibility(Some(&meta), None, &layout.fingerprint()),
            Err(ResumeDrift::CurrentDatasetSchemaUnavailable)
        );
    }

    #[test]
    fn resume_accepts_equivalent_schema_json_with_reordered_metadata_keys() {
        let layout = layout(&[("id", "int")]);
        // Same Arrow schema; metadata object key order differs.
        let stored = r#"{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{"b":"2","a":"1"}}],"metadata":{}}"#;
        let current = r#"{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{"a":"1","b":"2"}}],"metadata":{}}"#;
        let meta = CheckpointMeta::new(stored.to_string(), layout.fingerprint())
            .to_schema_json()
            .expect("serialize");
        check_resume_compatibility(Some(&meta), Some(current), &layout.fingerprint())
            .expect("equivalent schema JSON must resume despite metadata key order");
    }
}
