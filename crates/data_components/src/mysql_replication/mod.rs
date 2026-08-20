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
pub mod changes;
pub mod config;
pub mod gtid;
pub mod metrics;
pub mod resilience;
pub mod rows;
pub mod setup;
pub mod shared;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use snafu::Snafu;

use crate::cdc::{ChangeEnvelope, ChangesStream, NoOpCommitter, StreamError};

pub use config::{BinlogPosition, CursorType, ReplicationParams, derive_server_id, process_nonce};
pub use gtid::GtidSet;
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
        "MySQL account {account} is missing privileges required for change data capture: \
         {missing}. Grant them with: \
         GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO {grant_target};"
    ))]
    MissingPrivileges {
        account: String,
        grant_target: String,
        missing: String,
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

    #[snafu(display(
        "Table {database}.{table} is already replicated by another dataset on the same MySQL \
         connection ({connection}). `refresh_mode: changes` datasets on one connection share a \
         single binlog dump, so a source table can back at most one dataset — remove the \
         duplicate dataset."
    ))]
    SharedTableAlreadySubscribed {
        database: String,
        table: String,
        connection: String,
    },

    #[snafu(display(
        "Shared MySQL binlog connection ({connection}) is unavailable (its dump connection is \
         shutting down). Retry — a fresh shared connection will be established."
    ))]
    SharedSourceUnavailable { connection: String },

    #[snafu(display("Failed to parse MySQL GTID set: {message}"))]
    GtidParse { message: String },

    #[snafu(display(
        "Cannot resume MySQL replication for {dataset} ({database}.{table}): this dataset was \
         bootstrapped with GTID auto-positioning, but the source server no longer reports \
         `gtid_mode = ON` (it may have been reconfigured, or this is a different server without \
         GTIDs). Resuming by file+offset instead would silently start from a server-local \
         position that does not correspond to the applied GTID set. Either restore \
         `gtid_mode = ON` on the source (or repoint at a GTID-capable server) to resume via GTID, \
         or drop the accelerator's persisted state (its `spice_sys_mysql_binlog` row) to \
         re-bootstrap from scratch. \
         See: https://spiceai.org/docs/components/data-connectors/mysql"
    ))]
    GtidResumeUnavailable {
        dataset: String,
        database: String,
        table: String,
    },

    #[snafu(display(
        "MySQL replication for {dataset}: this dataset is positioning by GTID, but the source \
         emitted an anonymous transaction (no GTID). This means the source's `gtid_mode` is not \
         fully ON (e.g. ON_PERMISSIVE), so the applied GTID set cannot describe every \
         transaction. Set `gtid_mode = ON` on the source, or drop the accelerator's persisted \
         state to re-bootstrap by file+offset. \
         See: https://spiceai.org/docs/components/data-connectors/mysql"
    ))]
    AnonymousTransactionUnderGtid { dataset: String },
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
    /// Serialized executed [`GtidSet`] (`uuid:range` text) when positioning by
    /// GTID; may be empty (`gtid_mode = ON` but no transactions applied yet).
    /// `None` for file+offset positioning. This is the failover-safe resume
    /// identity: unlike `position` it is server-independent, so a checkpoint
    /// written against one primary resumes against a promoted replica via
    /// `COM_BINLOG_DUMP_GTID`.
    pub gtid_set: Option<String>,
    /// The checkpoint's cursor type, stored explicitly rather than inferred
    /// from `gtid_set` (an empty GTID set must still resume as GTID; see [`CursorType`])
    pub cursor_type: CursorType,
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
    /// Serialized dataset Arrow schema (the encoding `arrow_tools::schema::schema_to_json`
    /// produces).
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
    // `MySQL`'s binlog dump is server-wide with no server-side table filter, so a
    // dedicated per-dataset connection would just duplicate the whole stream for
    // no benefit. Every `refresh_mode: changes` dataset is therefore coalesced
    // onto one shared dump per connection identity ([`shared`]) — no opt-in, no
    // group label. A single dataset is simply a shared source with one member.
    // (A future per-dataset opt-out would not resurrect a second engine; it
    // would give that dataset a unique [`shared::SourceKey`] so it coalesces with
    // nothing — see the note there.)
    shared::subscribe(input)
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
