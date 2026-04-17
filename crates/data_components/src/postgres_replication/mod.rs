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

//! PostgreSQL logical replication for Spice acceleration.
//!
//! Streams WAL changes from a source Postgres database directly into the local
//! accelerator via the existing [`crate::cdc::ChangesStream`] abstraction.
//!
//! Entry point: [`start_replication_stream`].

pub mod bootstrap;
pub mod changes;
pub mod client;
pub mod config;
pub mod metrics;
pub mod pgoutput;
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

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to establish setup connection to Postgres: {source}"))]
    SetupConnect { source: tokio_postgres::Error },

    #[snafu(display("Failed to execute setup SQL: {source}"))]
    SetupExec { source: tokio_postgres::Error },

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

    #[snafu(display("Bootstrap stream error: {source}"))]
    Bootstrap { source: tokio_postgres::Error },
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
///   2. If this is a fresh slot, run a COPY-based snapshot of the source table.
///   3. Hand off to the `pgwire_replication::ReplicationClient` for streaming WAL.
///
/// Back-pressure: the returned stream waits for each envelope's `commit()` to
/// complete before emitting the next one, so the accelerator's write throughput
/// naturally paces the replication stream.
pub fn start_replication_stream(input: ReplicationStreamInput) -> ChangesStream {
    let confirmed_flush = Arc::new(AtomicU64::new(0));
    Box::pin(
        stream::once(async move { start_inner(input, confirmed_flush).await })
            .flat_map(|result| match result {
                Ok(stream) => stream,
                Err(e) => stream::once(async move { Err(stream_error(e)) }).boxed(),
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
    let outcome = slot::setup_slot_and_publication(&params, &schema_name, &table_name).await?;

    // 2. If the slot was just created and bootstrap is enabled, run snapshot.
    let bootstrap_stream = if outcome.created_fresh && params.initial_snapshot {
        Some(
            bootstrap::snapshot_stream(
                params.clone(),
                outcome.snapshot_name.clone(),
                schema_name.clone(),
                table_name.clone(),
                Arc::clone(&schema),
                primary_keys.clone(),
                dataset_name.clone(),
                Arc::clone(&metrics),
            )
            .await?,
        )
    } else {
        // No bootstrap this run — if the slot already existed, consider the
        // accelerator "already populated" so operators see bootstrap_complete=1.
        metrics.mark_bootstrap_complete();
        None
    };

    // 3. Start the WAL stream.
    let wal_stream = client::start_wal_stream(client::WalStreamInput {
        params,
        slot_name: outcome.slot_name.clone(),
        publication_name: outcome.publication_name.clone(),
        start_lsn: outcome.consistent_lsn,
        schema,
        primary_keys,
        dataset_name,
        is_dataset_ready_on_first_event: bootstrap_stream.is_none(),
        confirmed_flush,
        metrics,
    })
    .await?;

    Ok(match bootstrap_stream {
        Some(boot) => Box::pin(boot.chain(wal_stream)),
        None => wal_stream,
    })
}

fn stream_error(err: Error) -> StreamError {
    StreamError::External(err.to_string())
}

/// Helper for `async_stream::try_stream!` blocks to turn an [`Error`] into the
/// `StreamError` that the cdc machinery speaks.
pub(crate) fn err_to_stream(err: Error) -> StreamError {
    stream_error(err)
}
