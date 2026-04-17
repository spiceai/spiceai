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

//! Drive the `pgwire_replication::ReplicationClient` and emit
//! [`crate::cdc::ChangeEnvelope`]s per transaction.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use async_stream::try_stream;
use futures::Stream;
use secrecy::ExposeSecret;

use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};

use super::{
    Result, SchemaMismatchSnafu,
    changes::{TransactionBuffer, build_change_batch, envelope_with_lsn},
    config::ReplicationParams,
    pgoutput::{Decoder, DecodedMessage},
};
use crate::cdc::{ChangeEnvelope, ChangesStream, StreamError};

pub struct WalStreamInput {
    pub params: ReplicationParams,
    pub slot_name: String,
    pub publication_name: String,
    pub start_lsn: u64,
    pub schema: SchemaRef,
    pub primary_keys: Vec<String>,
    pub dataset_name: String,
    /// When `true`, the first envelope emitted will signal the dataset as
    /// ready — used when we skip bootstrap (existing slot resume path).
    pub is_dataset_ready_on_first_event: bool,
    pub confirmed_flush: Arc<AtomicU64>,
}

/// Establish the replication connection and return a `ChangesStream`.
pub async fn start_wal_stream(input: WalStreamInput) -> Result<ChangesStream> {
    let config = build_replication_config(&input);
    let client = ReplicationClient::connect(config)
        .await
        .map_err(|source| super::Error::StartReplication { source })?;

    Ok(Box::pin(wal_stream(client, input)))
}

fn build_replication_config(input: &WalStreamInput) -> ReplicationConfig {
    // TLS for replication is always disabled in this first pass. Users who
    // need TLS should terminate it upstream or we can add it in a follow-up;
    // the rustls feature is enabled on the crate already.
    let tls = TlsConfig::default();
    ReplicationConfig {
        host: input.params.host.clone(),
        port: input.params.port,
        user: input.params.user.clone(),
        password: input.params.password.expose_secret().to_string(),
        database: input.params.database.clone(),
        tls,
        slot: input.slot_name.clone(),
        publication: input.publication_name.clone(),
        start_lsn: Lsn(input.start_lsn),
        stop_at_lsn: None,
        status_interval: input.params.status_interval,
        idle_wakeup_interval: Duration::from_secs(1),
        buffer_events: 1024,
    }
}

fn wal_stream(
    mut client: ReplicationClient,
    input: WalStreamInput,
) -> impl Stream<Item = std::result::Result<ChangeEnvelope, StreamError>> + Send + use<> {
    let schema = input.schema;
    let dataset_name = input.dataset_name;
    let primary_keys = input.primary_keys;
    let confirmed_flush = Arc::clone(&input.confirmed_flush);
    let mark_ready_on_first = input.is_dataset_ready_on_first_event;

    try_stream! {
        let mut decoder = Decoder::new();
        let mut txn: Option<TransactionBuffer> = None;
        let mut first_emitted = !mark_ready_on_first;

        loop {
            let event = match client.recv().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => {
                    Err(StreamError::External(format!(
                        "postgres replication recv failed for {dataset_name}: {e}"
                    )))?;
                    unreachable!();
                }
            };

            match event {
                ReplicationEvent::Begin { final_lsn, .. } => {
                    txn = Some(TransactionBuffer::new(final_lsn.0));
                }
                ReplicationEvent::XLogData { data, .. } => {
                    let msg = decoder
                        .decode(&data)
                        .map_err(|e| StreamError::External(format!(
                            "pgoutput decode failed for {dataset_name}: {e}"
                        )))?;

                    match msg {
                        DecodedMessage::Relation(rel) => {
                            validate_relation_against_schema(&schema, &rel, &primary_keys)
                                .map_err(|e| StreamError::External(format!(
                                    "schema mismatch for {dataset_name}: {e}"
                                )))?;
                        }
                        DecodedMessage::Insert { relation_id, tuple } => {
                            let rel = decoder
                                .relation(relation_id)
                                .ok_or_else(|| StreamError::External(format!(
                                    "Insert before Relation for id {relation_id}"
                                )))?
                                .clone();
                            txn.get_or_insert_with(|| TransactionBuffer::new(0))
                                .push_insert(&rel, tuple);
                        }
                        DecodedMessage::Update { relation_id, new, .. } => {
                            let rel = decoder
                                .relation(relation_id)
                                .ok_or_else(|| StreamError::External(format!(
                                    "Update before Relation for id {relation_id}"
                                )))?
                                .clone();
                            txn.get_or_insert_with(|| TransactionBuffer::new(0))
                                .push_update(&rel, new);
                        }
                        DecodedMessage::Delete { relation_id, old } => {
                            let rel = decoder
                                .relation(relation_id)
                                .ok_or_else(|| StreamError::External(format!(
                                    "Delete before Relation for id {relation_id}"
                                )))?
                                .clone();
                            txn.get_or_insert_with(|| TransactionBuffer::new(0))
                                .push_delete(&rel, old);
                        }
                        DecodedMessage::Truncate { .. } => {
                            tracing::warn!(
                                dataset = %dataset_name,
                                "TRUNCATE received from postgres replication; skipping (not yet supported)"
                            );
                        }
                        // Begin/Commit should not come via XLogData with this
                        // client — they're already surfaced as distinct events.
                        DecodedMessage::Begin { .. } | DecodedMessage::Commit { .. } => {}
                        DecodedMessage::Other => {}
                    }
                }
                ReplicationEvent::Commit { end_lsn, .. } => {
                    if let Some(buffer) = txn.take()
                        && !buffer.is_empty()
                    {
                        let rel = decoder
                            .relation_iter()
                            .next()
                            .ok_or_else(|| StreamError::External(
                                "Commit without prior Relation".to_string(),
                            ))?;

                        let batch = build_change_batch(&schema, rel, &buffer.changes)
                            .map_err(|e| StreamError::External(format!(
                                "change batch build failed for {dataset_name}: {e}"
                            )))?;

                        let is_ready = !first_emitted;
                        first_emitted = true;

                        let envelope = envelope_with_lsn(
                            batch,
                            Arc::clone(&confirmed_flush),
                            end_lsn.0,
                            is_ready,
                        );
                        yield envelope;
                    } else {
                        // Empty transaction — still advance the LSN.
                        advance(&confirmed_flush, end_lsn.0);
                    }
                    // Forward the durable LSN to the replication client so it
                    // can send StandbyStatusUpdate in the background.
                    client.update_applied_lsn(Lsn(confirmed_flush.load(Ordering::Relaxed)));
                }
                ReplicationEvent::KeepAlive { wal_end, reply_requested, .. } => {
                    if reply_requested {
                        client.update_applied_lsn(wal_end);
                    }
                }
                ReplicationEvent::Message { .. } => {}
                ReplicationEvent::StoppedAt { reached } => {
                    tracing::info!(
                        dataset = %dataset_name,
                        reached = ?reached,
                        "replication stream stopped at upper bound"
                    );
                    break;
                }
            }
        }
    }
}

fn advance(flush: &AtomicU64, to: u64) {
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

fn validate_relation_against_schema(
    dataset_schema: &SchemaRef,
    rel: &super::pgoutput::Relation,
    declared_pks: &[String],
) -> Result<()> {
    for field in dataset_schema.fields() {
        if !rel.columns.iter().any(|c| c.name == *field.name()) {
            return SchemaMismatchSnafu {
                message: format!(
                    "column `{}` from dataset schema is missing in source relation {}.{}",
                    field.name(),
                    rel.namespace,
                    rel.name
                ),
            }
            .fail();
        }
    }
    for pk in declared_pks {
        let Some(col) = rel.columns.iter().find(|c| c.name == *pk) else {
            return SchemaMismatchSnafu {
                message: format!(
                    "declared primary_key `{pk}` not found on source relation {}.{}",
                    rel.namespace, rel.name
                ),
            }
            .fail();
        };
        if !col.is_key {
            return SchemaMismatchSnafu {
                message: format!(
                    "column `{pk}` is not part of source replica identity on {}.{}",
                    rel.namespace, rel.name
                ),
            }
            .fail();
        }
    }
    Ok(())
}
