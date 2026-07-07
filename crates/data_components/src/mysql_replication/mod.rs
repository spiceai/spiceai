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
    build_ready_signal_envelope,
};

pub use config::{
    BinlogPosition, InvalidPositionBehavior, ReplicationParams, SnapshotMode, derive_server_id,
    process_nonce,
};
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
/// plus an optional serialized Arrow schema snapshot for drift detection.
#[derive(Clone, Debug)]
pub struct PersistedPosition {
    pub position: BinlogPosition,
    pub schema_json: Option<String>,
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
    let persisted = position_store
        .load()
        .await
        .map_err(|e| Error::PositionStoreAccess {
            message: e.to_string(),
        })?;

    if let Some(persisted) = &persisted
        && let (Some(stored), Some(current)) =
            (persisted.schema_json.as_deref(), schema_json.as_deref())
        && stored != current
    {
        tracing::warn!(
            dataset = %dataset_name,
            "mysql replication resume detected dataset schema drift between runs; continuing \
             with the current schema. If columns fail to populate, re-bootstrap by restarting \
             with `mysql_replication_invalid_position_behavior: rebootstrap`."
        );
    }

    let resume_position = match persisted {
        Some(persisted) if params.snapshot_mode == SnapshotMode::Always => {
            tracing::info!(
                dataset = %dataset_name,
                position = %persisted.position,
                "`snapshot_mode: always`: running the initial snapshot despite a persisted \
                 binlog position"
            );
            None
        }
        Some(persisted) => {
            if setup::binlog_file_exists(&mut conn, &persisted.position.file).await? {
                Some(persisted.position)
            } else {
                match params.invalid_position_behavior {
                    InvalidPositionBehavior::Error => {
                        return StalePositionSnafu {
                            message: format!(
                                "persisted binlog position {} is no longer on the server \
                                 (binary logs were purged). Set \
                                 `mysql_replication_invalid_position_behavior: rebootstrap` to \
                                 drop the saved position and re-snapshot the table, or increase \
                                 `binlog_expire_logs_seconds` on the source.",
                                persisted.position
                            ),
                        }
                        .fail();
                    }
                    InvalidPositionBehavior::Rebootstrap => {
                        tracing::warn!(
                            dataset = %dataset_name,
                            position = %persisted.position,
                            "persisted binlog position was purged from the source; rebootstrap \
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
        // Resume: no snapshot. Signal readiness immediately — on a quiet
        // source the first binlog event may be arbitrarily far away.
        if let Err(e) = conn.disconnect().await {
            tracing::debug!(dataset = %dataset_name, error = %e, "setup connection disconnect");
        }
        tracing::info!(
            dataset = %dataset_name,
            position = %position,
            "mysql replication: resuming binlog stream from persisted position; skipping snapshot"
        );
        metrics.mark_bootstrap_complete();
        let ready = ready_envelope(&schema)?;
        (position, Box::pin(stream::once(async move { Ok(ready) })))
    } else {
        // Cold start: capture the binlog head BEFORE any snapshot so the
        // overlap replays idempotently.
        let head = setup::fetch_head_position(&mut conn).await?;
        // Seed snapshot progress from the source's approximate row count
        // (`information_schema.TABLES`) so operators get a progress signal;
        // best-effort — absence just leaves the metric unset.
        if params.snapshot_mode != SnapshotMode::Never {
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

        if params.snapshot_mode == SnapshotMode::Never {
            tracing::info!(
                dataset = %dataset_name,
                position = %head,
                "mysql replication: `snapshot_mode: never` — streaming changes from the \
                 current binlog head without snapshotting existing rows"
            );
            metrics.mark_bootstrap_complete();
            // Persist the start position up front: with no snapshot there is
            // no bootstrap barrier to piggy-back on, and resuming from `head`
            // after a restart is exactly the no-snapshot contract.
            let initial = PersistedPosition {
                position: head.clone(),
                schema_json: schema_json.clone(),
            };
            if let Err(e) = position_store.save(&initial).await {
                tracing::warn!(
                    dataset = %dataset_name,
                    error = %e,
                    "failed to persist the initial binlog position; a restart before the first \
                     checkpoint will re-attach at the then-current head"
                );
            }
            let ready = ready_envelope(&schema)?;
            (head, Box::pin(stream::once(async move { Ok(ready) })))
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

            // The captured head position commits piggy-backed on the
            // ready-signal envelope, after the runtime has durably applied
            // the whole snapshot. A crash before then leaves the sidecar
            // empty, so the next start re-bootstraps from scratch.
            let (_, ready_batch, is_ready) = ready_envelope(&schema)?.into_parts();
            let ready = ChangeEnvelope::from_parts(
                Box::new(InitialPositionCommitter {
                    store: Arc::clone(&position_store),
                    position: PersistedPosition {
                        position: head.clone(),
                        schema_json: schema_json.clone(),
                    },
                }),
                ready_batch,
                is_ready,
            );

            (
                head,
                Box::pin(
                    stream::once(async move { Ok(truncate) })
                        .chain(snapshot)
                        .chain(stream::once(async move { Ok(ready) })),
                ),
            )
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
        schema_json,
        metrics,
    });

    Ok(Box::pin(prelude.chain(binlog)))
}

/// Empty ready-signal envelope (no-op committer).
fn ready_envelope(schema: &SchemaRef) -> Result<ChangeEnvelope> {
    build_ready_signal_envelope(schema).map_err(|e| Error::SchemaMismatch {
        message: e.to_string(),
    })
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
}

#[async_trait]
impl CommitChange for InitialPositionCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.store
            .save(&self.position)
            .await
            .map_err(|source| CommitError::UnableToCommitChange { source })
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
