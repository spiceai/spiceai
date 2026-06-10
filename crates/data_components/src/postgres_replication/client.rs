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
    ReplicationMetricsCollector, Result, SchemaMismatchSnafu,
    changes::{TransactionBuffer, build_change_batch, envelope_with_lsn},
    config::{ReplicationParams, SslMode},
    pgoutput::{DecodedMessage, Decoder},
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
    pub metrics: Arc<ReplicationMetricsCollector>,
}

/// Establish the replication connection and return a `ChangesStream`.
///
/// The stream is resilient: transient network / TCP / Postgres-restart errors
/// are handled internally by reconnecting with exponential backoff. Only
/// *fatal* errors (authentication failure, slot dropped, schema mismatch) are
/// surfaced to the caller.
pub async fn start_wal_stream(input: WalStreamInput) -> Result<ChangesStream> {
    // Do one upfront connection attempt so startup errors (bad host, auth
    // failure, slot missing) surface immediately instead of being swallowed
    // by the reconnect loop. If it succeeds we hand the client to the stream;
    // if it fails with a transient error we still proceed into the resilient
    // loop so the dataset comes up once Postgres is reachable.
    let config = build_replication_config(
        &input.params,
        &input.slot_name,
        &input.publication_name,
        input.start_lsn,
    );
    let initial = ReplicationClient::connect(config.clone()).await;
    match initial {
        Ok(client) => Ok(Box::pin(wal_stream(Some(client), config, input))),
        Err(e) if super::resilience::is_transient_pgwire(&e) => {
            tracing::warn!(
                error = %e,
                "initial replication connect failed transiently; stream will reconnect in background"
            );
            input.metrics.inc_reconnect();
            Ok(Box::pin(wal_stream(None, config, input)))
        }
        Err(source) => Err(super::Error::StartReplication { source }),
    }
}

pub(crate) fn build_replication_config(
    params: &ReplicationParams,
    slot_name: &str,
    publication_name: &str,
    start_lsn: u64,
) -> ReplicationConfig {
    // Map our `SslMode` to pgwire-replication's `TlsConfig`. The crate uses
    // rustls and its own SslMode enum (Disabled / Require / VerifyCa /
    // VerifyFull), so we pick the matching constructor and pass the optional
    // CA path.
    let tls = match params.sslmode {
        // Prefer maps to plaintext for WAL streaming. Rationale:
        // pgwire-replication does not expose a safe "try TLS then fall back
        // to plaintext" path, so the only two honest mappings are Disabled
        // or Require. Since `Prefer` is our parsing default, silently
        // strengthening it into Require would break non-TLS dev/test
        // Postgres instances that the regular connector happily talks to
        // (the setup connection uses tokio_postgres's real Prefer
        // semantics). Matching libpq's "don't block on missing TLS" intent
        // and staying symmetric with the setup path is the safer default —
        // operators who want TLS on replication must pick Require,
        // VerifyCa, or VerifyFull explicitly.
        SslMode::Disable | SslMode::Prefer => TlsConfig::disabled(),
        SslMode::Require => TlsConfig::require(),
        SslMode::VerifyCa => TlsConfig::verify_ca(params.sslrootcert.clone()),
        SslMode::VerifyFull => TlsConfig::verify_full(params.sslrootcert.clone()),
    };
    ReplicationConfig {
        host: params.host.clone(),
        port: params.port,
        user: params.user.clone(),
        password: params.password.expose_secret().to_string(),
        database: params.database.clone(),
        tls,
        slot: slot_name.to_string(),
        publication: publication_name.to_string(),
        start_lsn: Lsn(start_lsn),
        stop_at_lsn: None,
        status_interval: params.status_interval,
        idle_wakeup_interval: Duration::from_secs(1),
        buffer_events: 1024,
    }
}

fn wal_stream(
    initial_client: Option<ReplicationClient>,
    config: ReplicationConfig,
    input: WalStreamInput,
) -> impl Stream<Item = std::result::Result<ChangeEnvelope, StreamError>> + Send + use<> {
    let schema = input.schema;
    let dataset_name = input.dataset_name;
    let primary_keys = input.primary_keys;
    let confirmed_flush = Arc::clone(&input.confirmed_flush);
    let mark_ready_on_first = input.is_dataset_ready_on_first_event;
    let metrics = input.metrics;

    try_stream! {
        let mut first_emitted = !mark_ready_on_first;
        // If the caller passed `None`, the upfront connect failed transiently
        // (see `start_wal_stream`) — count it as a prior failure so the next
        // successful connect emits an INFO "resumed" line.
        let initial_failed = initial_client.is_none();
        let mut client_slot: Option<ReplicationClient> = initial_client;
        let mut backoff = super::resilience::Backoff::default_for_stream();
        let mut last_emitted_commit_lsn = confirmed_flush.load(Ordering::Relaxed);
        // Counts consecutive failed connect/recv attempts in the current
        // outage cycle. Reset to 0 on each successful connect. Used to:
        //   - Demote repeat WARN noise to DEBUG once an outage is established
        //     (the first failure is still WARN so it's not lost).
        //   - Emit an INFO "resumed" log on recovery so operators get a
        //     positive signal — they currently have to infer recovery from
        //     the absence of further WARNs.
        let mut reconnect_attempts: u32 = u32::from(initial_failed);

        // Outer reconnect loop: runs until we hit a fatal error or the stream
        // reaches a natural end (rare — Postgres replication slots are
        // indefinite). Transient errors drop the current client and restart.
        'reconnect: loop {
            if crate::cdc::is_shutting_down() {
                tracing::info!(
                    dataset = %dataset_name,
                    "runtime shutdown; releasing replication connection and slot"
                );
                break 'reconnect;
            }
            // Ensure we have an open client. Reconnect with backoff on
            // transient failures.
            let mut client = match client_slot.take() {
                Some(c) => { backoff.reset(); c }
                None => {
                    loop {
                        match ReplicationClient::connect(config.clone()).await {
                            Ok(c) => {
                                backoff.reset();
                                if reconnect_attempts > 0 {
                                    tracing::info!(
                                        dataset = %dataset_name,
                                        attempts = reconnect_attempts,
                                        "replication connection resumed"
                                    );
                                    reconnect_attempts = 0;
                                }
                                break c;
                            }
                            Err(e) if super::resilience::is_transient_pgwire(&e) => {
                                metrics.inc_reconnect();
                                reconnect_attempts = reconnect_attempts.saturating_add(1);
                                log_transient_reconnect(
                                    reconnect_attempts,
                                    &dataset_name,
                                    &e.to_string(),
                                    backoff.current().as_millis(),
                                );
                                backoff.wait().await;
                            }
                            Err(e) => {
                                Err(StreamError::External(format!(
                                    "fatal replication connect failed for {dataset_name}: {e}"
                                )))?;
                                unreachable!();
                            }
                        }
                    }
                }
            };
            // Fresh connection: relation cache and any half-built transaction
            // are stale — Postgres will resend the Relation before the next
            // change anyway.
            let mut decoder = Decoder::new();
            let mut txn: Option<TransactionBuffer> = None;

        'recv: loop {
            if crate::cdc::is_shutting_down() {
                // Release the walsender (and the slot it holds) now rather
                // than at process exit — the shutdown drain phase can keep
                // the process alive for tens of seconds. Checked per event,
                // so the bound is one keepalive interval on a quiet source.
                drop(client);
                tracing::info!(
                    dataset = %dataset_name,
                    "runtime shutdown; released replication connection and slot"
                );
                break 'reconnect;
            }
            let event = match client.recv().await {
                Ok(Some(e)) => e,
                Ok(None) => break 'reconnect, // server closed cleanly
                Err(e) => {
                    metrics.inc_recv_error();
                    if super::resilience::is_transient_pgwire(&e) {
                        metrics.inc_reconnect();
                        reconnect_attempts = reconnect_attempts.saturating_add(1);
                        log_transient_reconnect(
                            reconnect_attempts,
                            &dataset_name,
                            &e.to_string(),
                            backoff.current().as_millis(),
                        );
                        // Drop this client, loop back to outer reconnect.
                        break 'recv;
                    }
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
                ReplicationEvent::XLogData { data, wal_end, .. } => {
                    metrics.set_server_wal_end(wal_end.0);
                    let msg = match decoder.decode(&data) {
                        Ok(m) => m,
                        Err(e) => {
                            metrics.inc_decode_error();
                            Err(StreamError::External(format!(
                                "pgoutput decode failed for {dataset_name}: {e}"
                            )))?;
                            unreachable!();
                        }
                    };

                    match msg {
                        DecodedMessage::Relation(rel) => {
                            if let Err(e) = validate_relation_against_schema(
                                &schema,
                                &rel,
                                &primary_keys,
                            ) {
                                metrics.inc_schema_mismatch_error();
                                Err(StreamError::External(format!(
                                    "schema mismatch for {dataset_name}: {e}"
                                )))?;
                            }
                            decoder.apply_declared_primary_keys(rel.relation_id, &primary_keys);
                        }
                        DecodedMessage::Insert { relation_id, tuple } => {
                            let rel = resolve_relation(&decoder, relation_id)?;
                            txn.get_or_insert_with(|| TransactionBuffer::new(0))
                                .push_insert(rel, tuple);
                            metrics.inc_insert();
                        }
                        DecodedMessage::Update { relation_id, new, .. } => {
                            let rel = resolve_relation(&decoder, relation_id)?;
                            txn.get_or_insert_with(|| TransactionBuffer::new(0))
                                .push_update(rel, new);
                            metrics.inc_update();
                        }
                        DecodedMessage::Delete { relation_id, old } => {
                            let rel = resolve_relation(&decoder, relation_id)?;
                            txn.get_or_insert_with(|| TransactionBuffer::new(0))
                                .push_delete(rel, old);
                            metrics.inc_delete();
                        }
                        DecodedMessage::Truncate { relation_ids } => {
                            metrics.inc_truncate();
                            // pgoutput gives us the explicit list of relation ids
                            // the TRUNCATE applies to. For a single-table
                            // publication (our default), there should be exactly
                            // one id. We error clearly on any other shape to
                            // avoid silently applying the truncate to the wrong
                            // table.
                            let relation_id = match relation_ids.as_slice() {
                                [id] => *id,
                                [] => {
                                    Err(StreamError::External(format!(
                                        "pgoutput TRUNCATE for {dataset_name} did not include any relation ids"
                                    )))?;
                                    unreachable!();
                                }
                                _ => {
                                    Err(StreamError::External(format!(
                                        "pgoutput TRUNCATE for {dataset_name} referenced {} relations; \
                                         this replication path requires exactly one relation per publication",
                                        relation_ids.len()
                                    )))?;
                                    unreachable!();
                                }
                            };
                            let rel = match resolve_relation(&decoder, relation_id) {
                                Ok(r) => r,
                                Err(e) => {
                                    Err(StreamError::External(format!(
                                        "pgoutput TRUNCATE for {dataset_name} references relation id {relation_id} \
                                         before its Relation message was cached: {e}"
                                    )))?;
                                    unreachable!();
                                }
                            };
                            txn.get_or_insert_with(|| TransactionBuffer::new(0))
                                .push_truncate(rel);
                            tracing::info!(
                                dataset = %dataset_name,
                                relation_id,
                                "TRUNCATE from postgres replication queued for accelerator"
                            );
                        }
                        // Begin/Commit should not come via XLogData with this
                        // client — they're already surfaced as distinct events.
                        // Grouped with Other because the handling is identical (no-op).
                        DecodedMessage::Begin { .. }
                        | DecodedMessage::Commit { .. }
                        | DecodedMessage::Other => {}
                    }
                }
                ReplicationEvent::Commit { end_lsn, commit_time_micros, .. } => {
                    metrics.inc_transaction();
                    // Postgres pgoutput timestamps are microseconds since
                    // 2000-01-01 UTC (PostgreSQL epoch). Convert to SystemTime so
                    // `replication_lag_ms` reflects true source-to-apply latency,
                    // not local ingest recency.
                    metrics.record_commit_watermark(pg_epoch_to_system_time(commit_time_micros));

                    if let Some(buffer) = txn.take()
                        && !buffer.is_empty()
                    {
                        // Guard: we assume the publication contains exactly one
                        // table. Cross-table changes in a single publication would
                        // mix relations into one ChangeBatch with the wrong schema.
                        let mut rels = decoder.relation_iter();
                        let rel = rels
                            .next()
                            .ok_or_else(|| StreamError::External(
                                "Commit without prior Relation".to_string(),
                            ))?;
                        if rels.next().is_some() {
                            Err(StreamError::External(format!(
                                "Dataset {dataset_name}: replication publication contains multiple \
                                 tables, which is not supported. Create a separate publication \
                                 per dataset (the default `spice_<dataset>_pub`)."
                            )))?;
                            unreachable!();
                        }

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
                        last_emitted_commit_lsn = end_lsn.0;
                        yield envelope;
                    } else {
                        // Empty transaction — still advance the LSN.
                        advance(&confirmed_flush, end_lsn.0);
                    }
                    // Forward the durable LSN to the replication client so it
                    // can send StandbyStatusUpdate in the background, and mirror
                    // it into the metrics collector for observability.
                    let applied = confirmed_flush.load(Ordering::Relaxed);
                    metrics.set_confirmed_flush_lsn(applied);
                    client.update_applied_lsn(Lsn(applied));
                }
                ReplicationEvent::KeepAlive { wal_end, reply_requested: _, .. } => {
                    metrics.set_server_wal_end(wal_end.0);
                    // KeepAlive `wal_end` can advance even when this publication
                    // emitted no table changes. If no decoded transaction is
                    // pending, all source changes visible to this stream up to
                    // `wal_end` are either applied or irrelevant to this dataset,
                    // so it is safe to let Postgres recycle retained WAL through
                    // that point. During a pending transaction, keep reporting the
                    // last applied commit LSN so we never ACK past buffered rows.
                    let applied = keepalive_applied_lsn(
                        &confirmed_flush,
                        txn.is_some(),
                        last_emitted_commit_lsn,
                        wal_end.0,
                    );
                    metrics.set_confirmed_flush_lsn(applied);
                    client.update_applied_lsn(Lsn(applied));
                }
                ReplicationEvent::Message { .. } => {}
                ReplicationEvent::StoppedAt { reached } => {
                    tracing::info!(
                        dataset = %dataset_name,
                        reached = ?reached,
                        "replication stream stopped at upper bound"
                    );
                    break 'reconnect;
                }
            }
        } // end 'recv

        // Inner 'recv loop broke on a transient error. Sleep with backoff
        // before the outer 'reconnect loop reconnects.
        backoff.wait().await;
        } // end 'reconnect
    } // end try_stream!
}

/// Convert a Postgres-epoch microsecond timestamp (from pgoutput Commit) into a
/// `SystemTime`. Postgres' epoch is 2000-01-01T00:00:00 UTC, not the Unix epoch.
pub(crate) fn pg_epoch_to_system_time(pg_micros: i64) -> std::time::SystemTime {
    // 30 years = 946_684_800 seconds between 1970-01-01 and 2000-01-01.
    const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;
    let total_micros = pg_micros + PG_EPOCH_UNIX_SECS * 1_000_000;
    match u64::try_from(total_micros) {
        Ok(pos) => std::time::UNIX_EPOCH + std::time::Duration::from_micros(pos),
        Err(_) => std::time::UNIX_EPOCH,
    }
}

/// Borrow the cached pgoutput `Relation`. The relation cache has already had
/// its key flags rewritten so that *only* the dataset's declared primary-key
/// columns are treated as keys.
///
/// Why: with `REPLICA IDENTITY FULL`, Postgres flags every column as key
/// (used to match the old tuple during DELETE/UPDATE). That would explode
/// `ChangeBatch.primary_keys` and include types the delete path can't handle
/// (floats, dates). The dataset config already tells us which columns are the
/// real PK — use that.
fn resolve_relation(
    decoder: &Decoder,
    relation_id: u32,
) -> std::result::Result<&super::pgoutput::Relation, StreamError> {
    decoder.relation(relation_id).ok_or_else(|| {
        StreamError::External(format!("change event before Relation for id {relation_id}"))
    })
}

/// Threshold at which we stop logging individual reconnect attempts at WARN
/// level. The first failure is WARN so an outage is visible immediately; on
/// every subsequent failure within the same outage we drop to DEBUG to keep
/// the log volume sublinear in outage duration. The recovery INFO log is the
/// signal operators should grep for at the end of an outage.
const RECONNECT_WARN_THRESHOLD: u32 = 1;

/// Whether a reconnect attempt at `attempt` should log at WARN level. Above
/// the threshold, attempts log at DEBUG to keep log volume sublinear in outage
/// duration. Extracted as a pure function so the level transition can be
/// unit-tested without standing up a tracing subscriber.
fn reconnect_logs_at_warn(attempt: u32) -> bool {
    attempt <= RECONNECT_WARN_THRESHOLD
}

/// Emit a per-attempt log for a transient connect/recv failure. The first
/// attempt of an outage cycle is WARN (so an outage is loud and greppable);
/// subsequent attempts are DEBUG to avoid flooding logs during long outages.
pub(crate) fn log_transient_reconnect(attempt: u32, dataset: &str, error: &str, retry_in_ms: u128) {
    if reconnect_logs_at_warn(attempt) {
        tracing::warn!(
            dataset = %dataset,
            attempt,
            retry_in_ms = %retry_in_ms,
            error = %error,
            "replication connection lost; reconnecting"
        );
    } else {
        tracing::debug!(
            dataset = %dataset,
            attempt,
            retry_in_ms = %retry_in_ms,
            error = %error,
            "replication connection still down; reconnecting"
        );
    }
}

fn keepalive_applied_lsn(
    confirmed_flush: &AtomicU64,
    transaction_pending: bool,
    last_emitted_commit_lsn: u64,
    wal_end: u64,
) -> u64 {
    let applied = confirmed_flush.load(Ordering::Acquire);
    if !transaction_pending && applied >= last_emitted_commit_lsn {
        advance(confirmed_flush, wal_end);
    }
    confirmed_flush.load(Ordering::Relaxed)
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

pub(crate) fn validate_relation_against_schema(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_advances_filtered_lsn_when_idle() {
        let confirmed = AtomicU64::new(100);

        let applied = keepalive_applied_lsn(&confirmed, false, 100, 250);

        assert_eq!(applied, 250);
        assert_eq!(confirmed.load(Ordering::Relaxed), 250);
    }

    #[test]
    fn keepalive_does_not_advance_past_uncommitted_emitted_envelope() {
        let confirmed = AtomicU64::new(100);

        let applied = keepalive_applied_lsn(&confirmed, false, 200, 250);

        assert_eq!(applied, 100);
        assert_eq!(confirmed.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn keepalive_does_not_advance_past_pending_transaction() {
        let confirmed = AtomicU64::new(100);

        let applied = keepalive_applied_lsn(&confirmed, true, 100, 250);

        assert_eq!(applied, 100);
        assert_eq!(confirmed.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn reconnect_first_attempt_logs_at_warn() {
        // The first failure of an outage cycle must stay at WARN so the
        // outage is visible in default-level logs. Demoting all attempts to
        // DEBUG would mean an outage is silent unless DEBUG is enabled.
        assert!(reconnect_logs_at_warn(1));
    }

    #[test]
    fn reconnect_subsequent_attempts_drop_to_debug() {
        // Every attempt after the first within the same outage cycle drops
        // to DEBUG. This is the volume-suppression behavior that #10971
        // requested: a 1-hour outage no longer floods the log with 3600+
        // WARN lines per dataset.
        for attempt in 2..=100 {
            assert!(
                !reconnect_logs_at_warn(attempt),
                "attempt {attempt} should log at DEBUG, not WARN",
            );
        }
    }

    #[test]
    fn reconnect_log_helper_handles_saturating_attempt_count() {
        // Helper should be callable across the full u32 range (including
        // the saturated max value) without panicking — the production
        // counter uses `saturating_add` so it can sit at u32::MAX for an
        // arbitrarily long outage.
        log_transient_reconnect(u32::MAX, "events", "connection refused", 500);
    }
}
