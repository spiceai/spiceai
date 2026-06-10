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
pub mod shared;
pub mod slot;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow::datatypes::SchemaRef;
use futures::{StreamExt, stream};
use snafu::Snafu;

use crate::cdc::{ChangesStream, StreamError};

pub use config::ReplicationParams;
pub use metrics::{Metrics as ReplicationMetrics, MetricsCollector as ReplicationMetricsCollector};
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    // Datasets that opted into slot sharing are multiplexed onto one
    // replication connection per (connection, slot) — see [`shared`].
    if input.params.shared {
        return shared::subscribe(input);
    }
    // Initialized to 0 until `start_inner` learns the effective start LSN from
    // slot setup. This matters: KeepAlive replies and `replication_lag_bytes`
    // both read from this atomic, and a pinned-at-0 value would report a wildly
    // inflated lag until the first Commit. `start_inner` seeds it before
    // handing the atomic to the WAL client.
    let confirmed_flush = Arc::new(AtomicU64::new(0));
    Box::pin(
        stream::once(async move { start_inner(input, confirmed_flush).await }).flat_map(|result| {
            match result {
                Ok(stream) => stream,
                Err(e) => stream::once(async move { Err(stream_error(&e)) }).boxed(),
            }
        }),
    )
}

async fn start_inner(
    input: ReplicationStreamInput,
    confirmed_flush: Arc<AtomicU64>,
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

    // 1. Set up slot and publication. This is idempotent: existing resources are reused.
    //    After this call, seed `confirmed_flush` with the slot's consistent LSN
    //    so KeepAlive replies before any commit don't ACK 0 (which would pin
    //    lag_bytes artificially high and, in the resume case, accidentally
    //    advance the slot backwards if we acted on it).
    let outcome = slot::setup_slot_and_publication(&params, &schema_name, &table_name).await?;
    if outcome.consistent_lsn > 0 {
        confirmed_flush.store(outcome.consistent_lsn, std::sync::atomic::Ordering::Release);
        metrics.set_confirmed_flush_lsn(outcome.consistent_lsn);
    }

    // 2. If the slot was just created and bootstrap is enabled, run snapshot.
    let bootstrap_stream = if outcome.created_fresh && params.initial_snapshot {
        Some(bootstrap::snapshot_stream(bootstrap::SnapshotInput {
            params: params.clone(),
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
            dataset_schema: Arc::clone(&schema),
            primary_keys: primary_keys.clone(),
            dataset_name: dataset_name.clone(),
            metrics: Arc::clone(&metrics),
        })?)
    } else {
        // No bootstrap this run — if the slot already existed, consider the
        // accelerator "already populated" so operators see bootstrap_complete=1.
        metrics.mark_bootstrap_complete();
        None
    };

    // When we skip bootstrap (slot resume or `initial_snapshot: false`), emit
    // an immediate empty `is_dataset_ready=true` envelope so the runtime marks
    // the dataset ready without having to wait for the first WAL change. On
    // quiet sources that wait could be indefinite.
    let skip_bootstrap_ready: Option<ChangesStream> = if bootstrap_stream.is_none() {
        let envelope = crate::cdc::build_ready_signal_envelope(&schema).map_err(|e| {
            Error::SchemaMismatch {
                message: e.to_string(),
            }
        })?;
        Some(Box::pin(futures::stream::once(async move { Ok(envelope) })))
    } else {
        None
    };

    // 3. Start the WAL stream.
    let wal_stream = client::start_wal_stream(client::WalStreamInput {
        params,
        slot_name: outcome.slot_name.clone(),
        publication_name: outcome.publication_name.clone(),
        start_lsn: outcome.consistent_lsn,
        schema: Arc::clone(&schema),
        primary_keys,
        dataset_name,
        // The dataset is already marked ready by `skip_bootstrap_ready` (if
        // bootstrap was skipped) or by the final bootstrap envelope.
        is_dataset_ready_on_first_event: false,
        confirmed_flush,
        metrics,
    })
    .await?;

    Ok(match (bootstrap_stream, skip_bootstrap_ready) {
        (Some(boot), _) => Box::pin(boot.chain(wal_stream)),
        (None, Some(ready)) => Box::pin(ready.chain(wal_stream)),
        (None, None) => wal_stream,
    })
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
