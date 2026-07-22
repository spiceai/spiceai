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

//! Drive the `COM_BINLOG_DUMP` stream and emit one
//! [`crate::cdc::ChangeEnvelope`] per committed source transaction.
//!
//! # Ack model
//!
//! Postgres replication acks by advancing a *server-side* slot cursor; `MySQL`
//! has no such cursor, so the ack is Spice's own persisted [`BinlogPosition`]:
//!
//!   - each envelope's committer records its transaction's end position in a
//!     shared [`AckState`] (in-memory, cheap — run by the runtime after the
//!     batch is durably applied),
//!   - the stream folds those acks (plus safe idle advances past
//!     foreign-table traffic, mirroring the Postgres keepalive advance) into
//!     a monotonic `resume` position,
//!   - and persists `resume` to the [`super::PositionStore`] every
//!     `checkpoint_interval`.
//!
//! A crash therefore replays at most `checkpoint_interval` of already-applied
//! changes, which the accelerator's PK upsert absorbs idempotently
//! (at-least-once, the same contract as the Postgres snapshot/WAL boundary).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use arrow::datatypes::SchemaRef;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use mysql_async::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, Value};

use super::config::{BinlogPosition, CursorType, ReplicationParams};
use super::gtid::GtidSet;
use super::metrics::MetricsCollector;
use super::rows::{TransactionBuffer, build_change_batch, normalize_binlog_value, truncate_change};
use super::setup::TableLayout;
use super::{CheckpointMeta, Error, PersistedPosition, PositionStore, Result};
use crate::cdc::{
    ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError, build_heartbeat_envelope,
};
use uuid::Uuid;

pub(super) struct BinlogStreamInput {
    pub params: ReplicationParams,
    pub layout: TableLayout,
    /// Position to start (or resume) streaming from.
    pub start: BinlogPosition,
    pub schema: SchemaRef,
    /// Dataset-declared primary key column names.
    pub primary_keys: Vec<String>,
    /// Dataset field index → source row-image index.
    pub column_map: Vec<usize>,
    pub database: String,
    pub table: String,
    pub dataset_name: String,
    pub position_store: Arc<dyn PositionStore>,
    /// Versioned checkpoint meta ([`super::CheckpointMeta`] JSON) persisted
    /// alongside each position — dataset schema + source-layout fingerprint
    /// for drift detection on resume.
    pub schema_json: Option<String>,
    pub metrics: Arc<MetricsCollector>,
    /// Whether to open the dump with GTID auto-positioning and persist the
    /// executed GTID set alongside the file position (failover-safe resume).
    pub use_gtid: bool,
    /// The executed GTID set to seed from — the source head's set on cold
    /// start, or the persisted set on resume. Extended as transactions commit.
    /// Empty when `use_gtid` is false.
    pub gtid_seed: GtidSet,
}

/// Highest transaction-end position whose envelope committer has run, plus the
/// executed GTID set for GTID auto-positioning. Shared between the stream
/// (reader) and every emitted envelope's committer (writers).
///
/// The GTID set unions each committed transaction's GTID. Under the in-order
/// commit contract the position logic already relies on
/// ([`may_safely_advance`]), when the resume position advances every committed
/// transaction up to it is in the set — so a free union stays exactly in step
/// with the durable cursor, never ahead of applied data.
struct AckState {
    committed: Mutex<Option<BinlogPosition>>,
    gtid: Mutex<GtidSet>,
}

impl AckState {
    /// Seed the executed set (source head or persisted resume set). Empty when
    /// not using GTID positioning.
    fn new(gtid_seed: GtidSet) -> Self {
        Self {
            committed: Mutex::new(None),
            gtid: Mutex::new(gtid_seed),
        }
    }

    fn advance(&self, to: &BinlogPosition) {
        let mut committed = self
            .committed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match committed.as_ref() {
            Some(current) if current >= to => {}
            _ => *committed = Some(to.clone()),
        }
    }

    fn committed(&self) -> Option<BinlogPosition> {
        self.committed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Fold a committed transaction's GTID into the executed set.
    fn add_gtid(&self, uuid: Uuid, gno: u64) {
        self.gtid
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .add(uuid, gno);
    }

    /// Snapshot the executed set for persistence / reconnect auto-positioning.
    fn gtid_snapshot(&self) -> GtidSet {
        self.gtid
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// `CommitChange` impl that advances the shared ack position. Persistence to
/// the sidecar happens on the stream's periodic checkpoint, so commits stay
/// as cheap as the Postgres LSN bump.
struct PositionCommitter {
    ack: Arc<AckState>,
    position: BinlogPosition,
    /// Dataset name, for the committer-progress log line.
    dataset: String,
    /// Source-commit timestamp (ms since the Unix epoch) of the transaction
    /// this commit acks; `None` when the source event carried no timestamp.
    source_commit_ts_ms: Option<i64>,
    /// This transaction's GTID (`source uuid`, sequence), when the source is
    /// GTID-enabled. Folded into the executed set on commit so the persisted
    /// cursor advances exactly with durably-applied transactions.
    gtid: Option<(Uuid, u64)>,
}

#[async_trait]
impl CommitChange for PositionCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.ack.advance(&self.position);
        if let Some((uuid, gno)) = self.gtid {
            self.ack.add_gtid(uuid, gno);
        }
        crate::cdc::log_committer_progress(
            "mysql",
            &self.dataset,
            &self.position.to_string(),
            self.source_commit_ts_ms,
        );
        Ok(())
    }

    /// Deferring only delays the in-memory ack (and thus the persisted
    /// checkpoint). A crash before the deferred commit re-streams the
    /// un-acked tail from the older persisted position — at-least-once via
    /// the idempotent PK upsert. Safe to defer.
    fn supports_deferral(&self) -> bool {
        true
    }
}

/// Whether the resume position may advance to `candidate` (the end of the
/// event just processed): only when no transaction is buffering and every
/// previously emitted envelope has been committed. Mirrors the Postgres
/// `advance_if_fully_acked` guard — never ack past rows the runtime hasn't
/// durably applied.
fn may_safely_advance(
    txn_pending: bool,
    last_emitted: Option<&BinlogPosition>,
    committed: Option<&BinlogPosition>,
) -> bool {
    if txn_pending {
        return false;
    }
    match (last_emitted, committed) {
        (None, _) => true,
        (Some(emitted), Some(committed)) => committed >= emitted,
        (Some(_), None) => false,
    }
}

fn advance_max(target: &mut BinlogPosition, candidate: BinlogPosition) {
    if candidate > *target {
        *target = candidate;
    }
}

pub(super) fn start_binlog_stream(input: BinlogStreamInput) -> ChangesStream {
    Box::pin(binlog_change_stream(input))
}

#[expect(
    clippy::too_many_lines,
    reason = "single state machine over the binlog event loop; splitting it would \
              scatter the per-connection state across helpers with 10+ arguments"
)]
fn binlog_change_stream(
    input: BinlogStreamInput,
) -> impl Stream<Item = std::result::Result<ChangeEnvelope, StreamError>> + Send + use<> {
    let BinlogStreamInput {
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
        use_gtid,
        gtid_seed,
    } = input;

    let pk_source_indexes = compute_pk_source_indexes(&schema, &primary_keys, &column_map);

    try_stream! {
        // The source table layout can be re-adopted mid-stream after a
        // compatible ALTER TABLE (see the DDL handling below), so the
        // positional mappings are mutable stream state.
        let mut layout = layout;
        let mut column_map = column_map;
        let mut pk_source_indexes = pk_source_indexes;
        let shutdown_epoch = crate::cdc::shutdown_epoch();
        let ack = Arc::new(AckState::new(gtid_seed));
        // Monotonic resume/ack position: what we reconnect from and persist.
        let mut resume = start.clone();
        let mut checkpointer = Checkpointer {
            store: position_store,
            schema_json,
            pending_adopt: None,
            dataset_name: dataset_name.clone(),
            metrics: Arc::clone(&metrics),
            last_persisted: start,
            use_gtid,
            // Seed from the resume set so the first checkpoint only fires once
            // the executed set actually grows past what was already persisted.
            last_persisted_gtid: ack.gtid_snapshot().to_string(),
        };
        // The GTID of the transaction group currently buffering, captured from
        // its GtidEvent and handed to the committer at commit time.
        let mut current_txn_gtid: Option<(Uuid, u64)> = None;
        let mut last_persist_at = Instant::now();
        let mut last_emitted: Option<BinlogPosition> = None;
        // Lazily-opened side connection for the periodic source-head poll
        // (`SHOW BINARY LOG STATUS`) behind the lag metrics; dropped and
        // reopened on error, never load-bearing for replication itself.
        let mut side_conn: Option<Conn> = None;
        let mut backoff = super::resilience::StreamBackoff::default_for_stream();
        let mut reconnect_attempts: u32 = 0;
        let ready_lag = params.ready_lag;
        // Idle readiness cadence: wake at least this often so a caught-up but
        // quiet source emits a lag-based readiness heartbeat within ~ready_lag,
        // even when the server streams its own (clock-less) binlog heartbeats.
        // Capped at the checkpoint interval so it never slows position saves.
        let idle_tick = crate::cdc::heartbeat_interval(ready_lag).min(params.checkpoint_interval);

        'reconnect: loop {
            if crate::cdc::shutdown_epoch() != shutdown_epoch {
                checkpointer.persist(&ack, &mut resume).await;
                tracing::info!(dataset = %dataset_name, "runtime shutdown; released binlog connection");
                break 'reconnect;
            }

            // Fold any acks that landed while we were disconnected.
            if let Some(committed) = ack.committed() {
                advance_max(&mut resume, committed);
            }

            let mut stream = match open_binlog_stream(
                &params,
                &resume,
                &dataset_name,
                use_gtid,
                &ack.gtid_snapshot(),
            )
            .await
            {
                Ok(stream) => {
                    backoff.reset();
                    if reconnect_attempts > 0 {
                        tracing::info!(
                            dataset = %dataset_name,
                            attempts = reconnect_attempts,
                            position = %resume,
                            "binlog connection resumed"
                        );
                        reconnect_attempts = 0;
                    }
                    stream
                }
                Err(e) if super::resilience::is_purged_position_error(&e) => {
                    Err(purged_position_error(&resume, &dataset_name))?;
                    unreachable!();
                }
                Err(e) if super::resilience::is_transient_mysql(&e) => {
                    metrics.inc_reconnect();
                    reconnect_attempts = reconnect_attempts.saturating_add(1);
                    log_transient_reconnect(
                        reconnect_attempts, &dataset_name, &e.to_string(),
                        backoff.next_delay().as_millis(),
                    );
                    backoff.wait().await;
                    continue 'reconnect;
                }
                Err(e) => {
                    Err(StreamError::External(format!(
                        "fatal mysql binlog connect failed for {dataset_name}: {e}"
                    )))?;
                    unreachable!();
                }
            };

            // Per-connection state. A fresh connection re-sends the rotate +
            // table-map events, so nothing carries over — except the decode
            // layout, which lives across reconnects. If we adopted mid-stream
            // but resume is still before that boundary, restore the pre-adopt
            // layout so replayed pre-DDL row images keep the matching map.
            if let Some(pre) = checkpointer.restore_pre_adopt_if_needed(&resume) {
                tracing::info!(
                    dataset = %dataset_name,
                    position = %resume,
                    "restoring pre-adopt source layout for reconnect before schema-change boundary"
                );
                layout = pre.layout.clone();
                column_map = pre.column_map.clone();
                pk_source_indexes = pre.pk_source_indexes.clone();
            }
            let mut current_file = resume.file.clone();
            let mut txn: Option<TransactionBuffer> = None;

            'recv: loop {
                if crate::cdc::shutdown_epoch() != shutdown_epoch {
                    // Release the dump thread now rather than at process
                    // exit; the shutdown drain phase can take tens of
                    // seconds. Checked per event and per idle tick, so the
                    // bound is one idle readiness tick on a quiet source.
                    if let Err(e) = stream.close().await {
                        tracing::debug!(dataset = %dataset_name, error = %e, "binlog stream close during shutdown");
                    }
                    checkpointer.persist(&ack, &mut resume).await;
                    tracing::info!(dataset = %dataset_name, "runtime shutdown; released binlog connection");
                    break 'reconnect;
                }

                // Bound the wait at the idle readiness tick so shutdown checks,
                // idle checkpointing, and readiness heartbeats never depend on
                // the server actually honoring the heartbeat request — a quiet
                // source with no server heartbeats must still reach Ready and
                // persist acked positions.
                let next_event = match tokio::time::timeout(idle_tick, stream.next()).await {
                    Ok(item) => item,
                    Err(_idle) => {
                        // Persist at the checkpoint cadence (a no-op when the
                        // position has not advanced).
                        if last_persist_at.elapsed() >= params.checkpoint_interval {
                            checkpointer.persist(&ack, &mut resume).await;
                            last_persist_at = Instant::now();
                        }
                        // When caught up to the source head, emit a lag-based
                        // readiness heartbeat stamped with a fresh source clock
                        // so a quiet, caught-up source still reaches Ready.
                        if let Some(source_now_ms) =
                            poll_source_head(&mut side_conn, &params, &resume, &metrics, &dataset_name)
                                .await
                        {
                            yield readiness_heartbeat(&schema, source_now_ms, ready_lag, &dataset_name)?;
                        }
                        continue 'recv;
                    }
                };
                let Some(event) = next_event else {
                    // Server closed the dump cleanly — treat as transient.
                    metrics.inc_recv_error();
                    metrics.inc_reconnect();
                    reconnect_attempts = reconnect_attempts.saturating_add(1);
                    log_transient_reconnect(
                        reconnect_attempts, &dataset_name,
                        "server closed the binlog stream",
                        backoff.next_delay().as_millis(),
                    );
                    break 'recv;
                };
                let event = match event {
                    Ok(event) => event,
                    Err(e) if super::resilience::is_purged_position_error(&e) => {
                        Err(purged_position_error(&resume, &dataset_name))?;
                        unreachable!();
                    }
                    Err(e) if super::resilience::is_transient_mysql(&e) => {
                        metrics.inc_recv_error();
                        metrics.inc_reconnect();
                        reconnect_attempts = reconnect_attempts.saturating_add(1);
                        log_transient_reconnect(
                            reconnect_attempts, &dataset_name, &e.to_string(),
                            backoff.next_delay().as_millis(),
                        );
                        break 'recv;
                    }
                    Err(e) => {
                        metrics.inc_recv_error();
                        Err(StreamError::External(format!(
                            "mysql binlog recv failed for {dataset_name}: {e}"
                        )))?;
                        unreachable!();
                    }
                };

                let header = event.header();
                let event_end_pos = u64::from(header.log_pos());
                let event_timestamp = header.timestamp();
                // A rotate event's header offset refers to the file being
                // left, while `current_file` flips to the new file below —
                // pairing them in the generic safe-advance would fabricate a
                // position that doesn't exist. Rotates advance explicitly in
                // their own arm instead.
                let is_rotate = matches!(
                    header.event_type(),
                    Ok(mysql_async::binlog::EventType::ROTATE_EVENT)
                );

                let data = match event.read_data() {
                    Ok(data) => data,
                    Err(e) => {
                        metrics.inc_decode_error();
                        Err(StreamError::External(format!(
                            "mysql binlog event decode failed for {dataset_name}: {e}"
                        )))?;
                        unreachable!();
                    }
                };

                // One ack read per event: committers run concurrently
                // downstream, so this is a snapshot — staleness only defers a
                // safe-advance to the next event. Both commit protocols set
                // `pending_commit`; the shared block after the match handles it.
                let committed = ack.committed();
                let mut pending_commit: Option<BinlogPosition> = None;

                match data {
                    Some(EventData::RotateEvent(rotate)) => {
                        let name = rotate.name().into_owned();
                        if !rotate.is_fake()
                            && may_safely_advance(txn.is_some(), last_emitted.as_ref(), committed.as_ref())
                        {
                            advance_max(&mut resume, BinlogPosition::new(name.clone(), rotate.position()));
                        }
                        current_file = name;
                    }
                    Some(EventData::TableMapEvent(tme)) => {
                        if table_map_matches(&tme, &database, &table)
                            && tme.columns_count() != layout.columns.len() as u64
                        {
                            // The table was altered by a statement this stream
                            // didn't witness (or couldn't parse). Try to adopt
                            // the current source layout — see the ALTER TABLE
                            // handling below for the tolerance contract.
                            let boundary =
                                BinlogPosition::new(current_file.clone(), event_end_pos);
                            // Reconnect replay: if we already recorded this
                            // boundary, apply that epoch — do NOT re-fetch
                            // today's information_schema (may be a later epoch).
                            if let Some(known) = checkpointer.layout_for_replay_boundary(&boundary) {
                                layout = known.layout.clone();
                                column_map = known.column_map.clone();
                                pk_source_indexes = known.pk_source_indexes.clone();
                            } else {
                            match adopt_current_layout(
                                &params, &database, &table, &schema, &layout, &primary_keys,
                                &dataset_name,
                            ).await {
                                Ok(adopted)
                                    if adopted.layout.columns.len() as u64
                                        == tme.columns_count() =>
                                {
                                    // Defer durable fingerprint update until
                                    // resume crosses this event — see
                                    // `Checkpointer::note_adopted_layout`. Keep
                                    // the pre-adopt layout so a reconnect that
                                    // reopens before the boundary can restore it.
                                    let pre_adopt = AdoptedLayout {
                                        layout: layout.clone(),
                                        column_map: column_map.clone(),
                                        pk_source_indexes: pk_source_indexes.clone(),
                                    };
                                    checkpointer.note_adopted_layout(
                                        &adopted,
                                        pre_adopt,
                                        &boundary,
                                    );
                                    layout = adopted.layout;
                                    column_map = adopted.column_map;
                                    pk_source_indexes = adopted.pk_source_indexes;
                                }
                                outcome => {
                                    // Do NOT persist the resume position past a
                                    // fatal schema boundary: advancing the
                                    // durable cursor would leave a restart
                                    // decoding remaining historical row images
                                    // with the current layout (silent column
                                    // scramble). Leave the last good checkpoint
                                    // in place so rebootstrap / operator fix
                                    // can recover from a known-safe position.
                                    metrics.inc_schema_mismatch_error();
                                    let reason = match outcome {
                                        Ok(adopted) => format!(
                                            "the current source layout has {} columns but this \
                                             event was recorded against {} — the stream is \
                                             replaying history from before a schema change",
                                            adopted.layout.columns.len(),
                                            tme.columns_count()
                                        ),
                                        Err(e) => e.to_string(),
                                    };
                                    Err(StreamError::External(format!(
                                        "mysql binlog for {dataset_name}: source table \
                                         {database}.{table} changed shape ({} columns on the \
                                         event, {} validated) and the new layout cannot be \
                                         adopted: {reason}. Re-bootstrap by setting \
                                         `mysql_replication_invalid_checkpoint_behavior: restart`.",
                                        tme.columns_count(),
                                        layout.columns.len()
                                    )))?;
                                    unreachable!();
                                }
                            }
                            }
                        }
                    }
                    Some(EventData::RowsEvent(rows_data)) => {
                        if let Some(tme) = stream
                            .get_tme(rows_data.table_id())
                            .filter(|tme| table_map_matches(tme, &database, &table))
                        {
                            let buffer = txn.get_or_insert_with(TransactionBuffer::new);
                            if let Err(e) = buffer_rows_event(
                                &rows_data, tme, &layout, &pk_source_indexes, buffer, &metrics,
                            ) {
                                metrics.inc_decode_error();
                                Err(StreamError::External(format!(
                                    "mysql binlog row decode failed for {dataset_name}: {e}"
                                )))?;
                                unreachable!();
                            }
                        }
                    }
                    Some(EventData::XidEvent(_)) => {
                        pending_commit = Some(BinlogPosition::new(current_file.clone(), event_end_pos));
                    }
                    Some(EventData::QueryEvent(query)) => {
                        let statement = query.query();
                        let default_db = query.schema();
                        match classify_query(&statement) {
                            QueryKind::Begin => {
                                txn = Some(TransactionBuffer::new());
                            }
                            QueryKind::Commit => {
                                // Non-InnoDB tables commit via a plain COMMIT
                                // query instead of an Xid event.
                                pending_commit = Some(BinlogPosition::new(current_file.clone(), event_end_pos));
                            }
                            QueryKind::Xa => {
                                // XA transactions commit via `XA COMMIT`, which
                                // this stream does not track — changes made to
                                // the subscribed table inside one would be
                                // dropped. Loud and per-statement rather than
                                // silent data loss.
                                if txn.as_ref().is_some_and(|t| !t.is_empty()) {
                                    Err(StreamError::External(format!(
                                        "mysql binlog for {dataset_name}: XA transaction touched \
                                         {database}.{table} ({statement}). XA (two-phase) \
                                         transactions are not supported by `refresh_mode: changes` \
                                         — use regular transactions for this table."
                                    )))?;
                                    unreachable!();
                                }
                                tracing::warn!(
                                    dataset = %dataset_name,
                                    statement = %statement,
                                    "XA transaction statement observed on the binlog; XA \
                                     transactions are not supported and their changes to other \
                                     tables are ignored"
                                );
                            }
                            QueryKind::Statement => {
                                match classify_statement(&statement, &default_db, &database, &table) {
                                    Some(StatementKind::Truncate) => {
                                        // TRUNCATE is DDL: auto-committed, never
                                        // inside a row transaction.
                                        metrics.inc_truncate();
                                        metrics.inc_transaction();
                                        record_watermark(&metrics, event_timestamp);
                                        let commit_pos = BinlogPosition::new(current_file.clone(), event_end_pos);
                                        let batch = build_change_batch(&schema, &primary_keys, &column_map, &[truncate_change()])
                                            .map_err(|e| StreamError::External(format!(
                                                "change batch build failed for {dataset_name}: {e}"
                                            )))?
                                            .with_source_commit_ts_ms(commit_ts_ms(event_timestamp));
                                        tracing::info!(
                                            dataset = %dataset_name,
                                            "TRUNCATE from mysql binlog queued for accelerator"
                                        );
                                        let envelope = ChangeEnvelope::new(
                                            Box::new(PositionCommitter {
                                                ack: Arc::clone(&ack),
                                                position: commit_pos.clone(),
                                                dataset: dataset_name.clone(),
                                                source_commit_ts_ms: commit_ts_ms(event_timestamp),
                                                // TRUNCATE is auto-committed inside its own
                                                // GTID group — fold that GTID into the set.
                                                gtid: current_txn_gtid.take(),
                                            }),
                                            batch,
                                            crate::cdc::source_commit_within_ready_lag(
                                                commit_ts_ms(event_timestamp),
                                                ready_lag,
                                            ),
                                        );
                                        last_emitted = Some(commit_pos);
                                        yield envelope;
                                    }
                                    // ALTER TABLE: re-fetch the source layout and keep
                                    // streaming when every dataset column still maps —
                                    // the same tolerance the Postgres connector's block
                                    // mode has for compatible relation changes. Columns
                                    // the source gained are not replicated (warned, in
                                    // `adopt_current_layout`); a dropped or retyped
                                    // dataset column is fatal below.
                                    Some(StatementKind::SchemaChange("ALTER TABLE")) => {
                                        let boundary = BinlogPosition::new(
                                            current_file.clone(),
                                            event_end_pos,
                                        );
                                        // Reconnect replay of a known boundary:
                                        // apply the pending epoch, do not
                                        // re-fetch today's information_schema.
                                        if let Some(known) =
                                            checkpointer.layout_for_replay_boundary(&boundary)
                                        {
                                            layout = known.layout.clone();
                                            column_map = known.column_map.clone();
                                            pk_source_indexes = known.pk_source_indexes.clone();
                                        } else {
                                        match adopt_current_layout(
                                            &params, &database, &table, &schema, &layout,
                                            &primary_keys, &dataset_name,
                                        ).await {
                                            Ok(adopted) => {
                                                let pre_adopt = AdoptedLayout {
                                                    layout: layout.clone(),
                                                    column_map: column_map.clone(),
                                                    pk_source_indexes: pk_source_indexes.clone(),
                                                };
                                                checkpointer.note_adopted_layout(
                                                    &adopted,
                                                    pre_adopt,
                                                    &boundary,
                                                );
                                                layout = adopted.layout;
                                                column_map = adopted.column_map;
                                                pk_source_indexes = adopted.pk_source_indexes;
                                            }
                                            Err(e) => {
                                                // Do NOT persist past a fatal ALTER —
                                                // see the TableMap mismatch path.
                                                metrics.inc_schema_mismatch_error();
                                                Err(StreamError::External(format!(
                                                    "mysql binlog for {dataset_name}: ALTER TABLE on source table \
                                                     {database}.{table} (statement: {statement}) cannot be adopted \
                                                     mid-stream: {e}. Update the dataset schema to match the new \
                                                     table definition, or re-bootstrap by setting \
                                                     `mysql_replication_invalid_checkpoint_behavior: restart`."
                                                )))?;
                                                unreachable!();
                                            }
                                        }
                                        }
                                    }
                                    Some(StatementKind::SchemaChange(verb)) => {
                                        // Do NOT persist past DROP/RENAME — see
                                        // the TableMap mismatch path.
                                        metrics.inc_schema_mismatch_error();
                                        Err(StreamError::External(format!(
                                            "mysql binlog for {dataset_name}: {verb} detected on source table \
                                             {database}.{table} (statement: {statement}). The subscribed table \
                                             no longer exists under this name — fix the source (or the dataset) \
                                             and re-bootstrap by setting \
                                             `mysql_replication_invalid_checkpoint_behavior: restart`."
                                        )))?;
                                        unreachable!();
                                    }
                                    None => {}
                                }
                                // An auto-committed statement closes the GTID
                                // transaction group it arrived in; drop the
                                // (necessarily empty — row changes never
                                // arrive as statements under ROW format)
                                // buffer so the safe-advance isn't wedged.
                                if txn.as_ref().is_some_and(TransactionBuffer::is_empty) {
                                    txn = None;
                                }
                            }
                        }
                    }
                    // A GTID event opens a transaction *group* ahead of its
                    // BEGIN/statement — start buffering here so the
                    // safe-advance can't checkpoint between the GTID and its
                    // transaction. Capture the GTID so the commit folds it into
                    // the executed set for failover-safe resume.
                    Some(EventData::GtidEvent(gtid_event)) => {
                        txn = Some(TransactionBuffer::new());
                        current_txn_gtid =
                            Some((Uuid::from_bytes(gtid_event.sid()), gtid_event.gno()));
                    }
                    Some(EventData::AnonymousGtidEvent(_)) => {
                        // An anonymous transaction carries no GTID. When this
                        // dataset is positioning by GTID it must not happen
                        // (source not fully `gtid_mode = ON`) — fail loudly
                        // rather than silently persist a GTID set that can't
                        // describe this transaction.
                        if use_gtid {
                            metrics.inc_decode_error();
                            Err(super::err_to_stream(Error::AnonymousTransactionUnderGtid {
                                dataset: dataset_name.clone(),
                            }))?;
                        }
                        txn = Some(TransactionBuffer::new());
                        current_txn_gtid = None;
                    }
                    // Heartbeats (and any other event) fall through to the
                    // safe-advance below, mirroring the Postgres KeepAlive
                    // handling.
                    Some(_) | None => {}
                }

                // Shared commit handling for both commit protocols (InnoDB
                // Xid event, non-InnoDB COMMIT query): emit the buffered
                // transaction, or fold an empty/foreign one into the
                // safe-advance.
                if let Some(commit_pos) = pending_commit {
                    let txn_gtid = current_txn_gtid.take();
                    if let Some(envelope) = commit_transaction(
                        &mut txn, &commit_pos, event_timestamp, &schema, &primary_keys,
                        &column_map, &ack, &metrics, &dataset_name, ready_lag, txn_gtid,
                    )? {
                        last_emitted = Some(commit_pos);
                        yield envelope;
                    } else if may_safely_advance(false, last_emitted.as_ref(), committed.as_ref()) {
                        advance_max(&mut resume, commit_pos);
                    }
                }

                // Safe idle advance: with no transaction buffering and every
                // emitted envelope committed, everything up to this event's
                // end is either applied or irrelevant to this dataset.
                if !is_rotate
                    && event_end_pos >= MIN_VALID_EVENT_POS
                    && may_safely_advance(txn.is_some(), last_emitted.as_ref(), committed.as_ref())
                {
                    // Same-file fast path: skip allocating a position per
                    // pass-through event.
                    if resume.file == current_file {
                        resume.pos = resume.pos.max(event_end_pos);
                    } else {
                        advance_max(&mut resume, BinlogPosition::new(current_file.clone(), event_end_pos));
                    }
                } else if let Some(committed) = committed {
                    advance_max(&mut resume, committed);
                }

                // Flush when the checkpoint interval elapses, or as soon as a
                // pending layout-adopt fingerprint becomes eligible (resume
                // crossed the DDL/TableMap boundary). The latter must not wait
                // for the interval — a crash with the old fingerprint still
                // durable is fine (forces rebootstrap); a crash with the *new*
                // fingerprint at a pre-boundary position is not.
                let flush_adopt = checkpointer.pending_adopt_ready(&resume);
                if flush_adopt || last_persist_at.elapsed() >= params.checkpoint_interval {
                    checkpointer.persist(&ack, &mut resume).await;
                    // A layout-adopt flush fires off-interval only to durably
                    // record the new fingerprint — skip the head
                    // poll/heartbeat there. On a regular interval flush, poll the
                    // source head and, when caught up, emit a lag-based readiness
                    // heartbeat.
                    if !flush_adopt
                        && let Some(source_now_ms) =
                            poll_source_head(&mut side_conn, &params, &resume, &metrics, &dataset_name)
                                .await
                    {
                        yield readiness_heartbeat(&schema, source_now_ms, ready_lag, &dataset_name)?;
                    }
                    last_persist_at = Instant::now();
                }
            } // 'recv

            // Inner loop broke on a transient error: back off, then let the
            // outer loop reconnect from the resume position.
            backoff.wait().await;
        } // 'reconnect
    }
}

/// Binlog events start at offset 4 (after the magic header); positions below
/// that (fake rotates and heartbeats report 0) are not resumable.
const MIN_VALID_EVENT_POS: u64 = 4;

async fn open_binlog_stream(
    params: &ReplicationParams,
    resume: &BinlogPosition,
    dataset_name: &str,
    use_gtid: bool,
    gtid: &GtidSet,
) -> std::result::Result<BinlogStream, mysql_async::Error> {
    let mut conn = Conn::new(params.opts.clone()).await?;

    // Ask the source to send heartbeat events while idle so the stream can
    // detect dead connections and advance its checkpoint. Half the
    // checkpoint interval (min 500ms) keeps idle persists within ~1.5×
    // the interval. The session variable is in nanoseconds; MySQL 8.4
    // renamed the replica-facing vocabulary, so set both spellings (unknown
    // user variables are inert).
    let heartbeat_nanos = (params.checkpoint_interval / 2)
        .max(Duration::from_millis(500))
        .as_nanos()
        .min(u128::from(u64::MAX));
    // Two separate statements: if a server rejects one spelling, the other
    // must still take effect (a combined statement fails atomically).
    for var in ["master_heartbeat_period", "source_heartbeat_period"] {
        if let Err(e) = mysql_async::prelude::Queryable::query_drop(
            &mut conn,
            format!("SET @{var} = {heartbeat_nanos}"),
        )
        .await
        {
            tracing::debug!(dataset = %dataset_name, error = %e, "failed to set @{var}");
        }
    }

    if use_gtid {
        // GTID auto-positioning: the server computes the start point from the
        // executed set (everything NOT in it is sent), so no filename/offset is
        // needed. This is what survives a failover — the set is
        // server-independent.
        conn.get_binlog_stream(
            BinlogStreamRequest::new(params.server_id)
                .with_gtid()
                .with_gtid_set(gtid.to_sids()),
        )
        .await
    } else {
        let pos_u32 = u32::try_from(resume.pos).unwrap_or(u32::MAX);
        conn.get_binlog_stream(
            BinlogStreamRequest::new(params.server_id)
                .with_filename(resume.file.as_bytes())
                .with_pos(u64::from(pos_u32)),
        )
        .await
    }
}

fn table_map_matches(tme: &TableMapEvent<'_>, database: &str, table: &str) -> bool {
    tme.database_name() == database && tme.table_name() == table
}

/// Decode a rows event for the subscribed table into the transaction buffer.
fn buffer_rows_event(
    rows_data: &RowsEventData<'_>,
    tme: &TableMapEvent<'_>,
    layout: &TableLayout,
    pk_source_indexes: &[usize],
    buffer: &mut TransactionBuffer,
    metrics: &MetricsCollector,
) -> Result<()> {
    #[derive(Clone, Copy)]
    enum RowOp {
        Insert,
        Update,
        Delete,
    }
    let op = match rows_data {
        RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => RowOp::Insert,
        RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_) => RowOp::Update,
        RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => RowOp::Delete,
        RowsEventData::PartialUpdateRowsEvent(_) => {
            return Err(Error::Decode {
                message: "partial-JSON row images are not supported. Set \
                          `binlog_row_value_options = ''` on the source server."
                    .to_string(),
            });
        }
    };

    // `RowsEventData::rows` unifies the V1/V2 event variants.
    for row in rows_data.rows(tme) {
        let (before, after) = row.map_err(row_io_error)?;
        match op {
            RowOp::Insert => {
                let after = required_image(after, "write", "after")?;
                buffer.push_insert(binlog_row_to_values(after, layout)?);
                metrics.inc_insert();
            }
            RowOp::Update => {
                let before = required_image(before, "update", "before")?;
                let after = required_image(after, "update", "after")?;
                buffer.push_update(
                    pk_source_indexes,
                    binlog_row_to_values(before, layout)?,
                    binlog_row_to_values(after, layout)?,
                );
                metrics.inc_update();
            }
            RowOp::Delete => {
                let before = required_image(before, "delete", "before")?;
                buffer.push_delete(binlog_row_to_values(before, layout)?);
                metrics.inc_delete();
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "used as a function pointer in map_err; taking by reference would require a closure at every call site"
)]
fn row_io_error(e: std::io::Error) -> Error {
    Error::Decode {
        message: format!("row image parse: {e}"),
    }
}

fn required_image(image: Option<BinlogRow>, op: &str, side: &str) -> Result<BinlogRow> {
    image.ok_or_else(|| Error::Decode {
        message: format!("{op} event is missing its {side} row image"),
    })
}

/// Convert a full binlog row image into per-source-column [`Value`]s.
fn binlog_row_to_values(mut row: BinlogRow, layout: &TableLayout) -> Result<Vec<Value>> {
    if row.len() != layout.columns.len() {
        return Err(Error::Decode {
            message: format!(
                "row image has {} columns but the validated layout has {} — the source \
                 table was altered. Restart the dataset to re-validate the schema.",
                row.len(),
                layout.columns.len()
            ),
        });
    }
    (0..row.len())
        .map(|idx| {
            let value = row.take(idx).ok_or_else(|| Error::Decode {
                message: format!(
                    "column #{idx} (`{}`) is absent from the row image. Spice requires \
                     `binlog_row_image = FULL` — a writer session overrode it.",
                    layout.columns[idx].name
                ),
            })?;
            normalize_binlog_value(&layout.columns[idx], value)
        })
        .collect()
}

/// Finish the buffered transaction: build the change batch and wrap it in an
/// envelope whose committer acks `commit_pos`. Returns `None` for an empty
/// (foreign-table or no-op) transaction.
#[expect(
    clippy::too_many_arguments,
    reason = "commit sites pass the stream's live state; bundling into a struct would \
              just relocate the argument list"
)]
fn commit_transaction(
    txn: &mut Option<TransactionBuffer>,
    commit_pos: &BinlogPosition,
    event_timestamp: u32,
    schema: &SchemaRef,
    primary_keys: &[String],
    column_map: &[usize],
    ack: &Arc<AckState>,
    metrics: &MetricsCollector,
    dataset_name: &str,
    ready_lag: Duration,
    txn_gtid: Option<(Uuid, u64)>,
) -> std::result::Result<Option<ChangeEnvelope>, StreamError> {
    metrics.inc_transaction();
    record_watermark(metrics, event_timestamp);

    let Some(buffer) = txn.take() else {
        return Ok(None);
    };
    if buffer.is_empty() {
        return Ok(None);
    }

    let batch = build_change_batch(schema, primary_keys, column_map, &buffer.changes)
        .map_err(|e| {
            StreamError::External(format!("change batch build failed for {dataset_name}: {e}"))
        })?
        .with_source_commit_ts_ms(commit_ts_ms(event_timestamp));

    // Lag-based readiness: mark Ready only when this commit's source time is
    // within `ready_lag` of now, i.e. the stream has caught up to the head.
    Ok(Some(ChangeEnvelope::new(
        Box::new(PositionCommitter {
            ack: Arc::clone(ack),
            position: commit_pos.clone(),
            dataset: dataset_name.to_string(),
            source_commit_ts_ms: commit_ts_ms(event_timestamp),
            gtid: txn_gtid,
        }),
        batch,
        crate::cdc::source_commit_within_ready_lag(commit_ts_ms(event_timestamp), ready_lag),
    )))
}

/// Build an idle readiness heartbeat: a zero-row envelope stamped with a
/// source-attested clock (`source_now_ms`), flagged Ready when that clock is
/// within `ready_lag` of now. Emitted only when the stream has caught up to the
/// source head, so it never marks a still-behind dataset Ready.
fn readiness_heartbeat(
    schema: &SchemaRef,
    source_now_ms: i64,
    ready_lag: Duration,
    dataset_name: &str,
) -> std::result::Result<ChangeEnvelope, StreamError> {
    let is_ready = crate::cdc::source_commit_within_ready_lag(Some(source_now_ms), ready_lag);
    // Log the idle heartbeat so lag-based readiness can be verified from the logs
    // (target spice_cdc::heartbeat). Covers both call sites of this helper.
    let lag_ms = crate::cdc::replication_lag_ms(Some(source_now_ms));
    tracing::debug!(
        target: "spice_cdc::heartbeat",
        connector = "mysql",
        dataset = %dataset_name,
        source_commit_ts_ms = source_now_ms,
        is_dataset_ready = is_ready,
        lag_ms = ?lag_ms,
        "CDC idle heartbeat emitted"
    );
    build_heartbeat_envelope(schema, Some(source_now_ms), is_ready).map_err(|e| {
        StreamError::External(format!(
            "heartbeat envelope build failed for {dataset_name}: {e}"
        ))
    })
}

fn record_watermark(metrics: &MetricsCollector, event_timestamp: u32) {
    if event_timestamp > 0 {
        metrics.record_commit_watermark(
            SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(event_timestamp)),
        );
    }
}

fn commit_ts_ms(event_timestamp: u32) -> Option<i64> {
    (event_timestamp > 0).then(|| i64::from(event_timestamp) * 1000)
}

/// One schema-change boundary and the layout that applies to events at/after it.
#[derive(Clone)]
struct LayoutEpoch {
    /// End position of the ALTER / `TableMap` that introduced this layout.
    boundary: BinlogPosition,
    layout: AdoptedLayout,
    fingerprint: String,
}

/// A mid-stream layout adopt (or chain of adopts) whose fingerprint must not
/// become durable until resume has crossed the relevant schema-change event.
///
/// Pairing a post-adopt fingerprint with a pre-boundary position would let a
/// crash/restart decode historical row images with the new ordinal map.
///
/// The in-memory decode layout is swapped immediately (same-connection events
/// after the DDL already use the new `TableMap`). On an in-process reconnect
/// that reopens from a still-pre-boundary `resume`, the layout that was in
/// force at that position is restored from [`Self::epochs`] /
/// [`Self::pre_adopt`].
struct PendingLayoutAdopt {
    /// Layout in force before the first pending epoch — restored when
    /// `resume` is still before `epochs[0].boundary`.
    pre_adopt: AdoptedLayout,
    /// Ordered schema-change epochs (ascending boundary). The layout for a
    /// resume position `R` is the last epoch with `boundary <= R`, or
    /// `pre_adopt` if `R` is before the first boundary.
    epochs: Vec<LayoutEpoch>,
}

impl PendingLayoutAdopt {
    fn earliest_boundary(&self) -> Option<&BinlogPosition> {
        self.epochs.first().map(|epoch| &epoch.boundary)
    }

    fn latest_boundary(&self) -> Option<&BinlogPosition> {
        self.epochs.last().map(|epoch| &epoch.boundary)
    }

    /// Layout that was in force at `resume` (for reconnect restore).
    fn layout_at(&self, resume: &BinlogPosition) -> &AdoptedLayout {
        let mut current = &self.pre_adopt;
        for epoch in &self.epochs {
            if resume < &epoch.boundary {
                break;
            }
            current = &epoch.layout;
        }
        current
    }

    /// Fingerprint that matches `resume` (for durable checkpoint meta).
    fn fingerprint_at(&self, resume: &BinlogPosition) -> Option<&str> {
        let mut fingerprint: Option<&str> = None;
        for epoch in &self.epochs {
            if resume < &epoch.boundary {
                break;
            }
            fingerprint = Some(epoch.fingerprint.as_str());
        }
        fingerprint
    }
}

/// Owns the durable half of the ack pipeline: folds committer acks into the
/// resume position and writes it to the position store when it advanced.
struct Checkpointer {
    store: Arc<dyn PositionStore>,
    /// Versioned [`CheckpointMeta`] JSON (dataset schema + source-layout
    /// fingerprint). The fingerprint is updated from [`Self::pending_adopt`]
    /// only once resume has crossed the schema-change boundary.
    schema_json: Option<String>,
    /// Compatible mid-stream adopt(s) waiting for resume to pass their
    /// boundaries before the matching fingerprint is written to the sidecar.
    pending_adopt: Option<PendingLayoutAdopt>,
    dataset_name: String,
    metrics: Arc<MetricsCollector>,
    last_persisted: BinlogPosition,
    /// Persist the executed GTID set alongside the file position (failover-safe
    /// resume). When false, `gtid_set` stays `None` and resume is file+offset.
    use_gtid: bool,
    /// The executed GTID set (serialized) last durably persisted. Under GTID the
    /// set is the authoritative cursor: a failover can repoint the stream at a
    /// server whose binlog file ordinals are *lower* than the persisted one, so
    /// `resume` (ordered by file ordinal) may never advance again even as the
    /// set keeps growing. Gating persistence on this too — not only on `resume`
    /// — keeps the failover-safe GTID checkpoint advancing so the crash-replay
    /// window stays bounded. Empty (and unused) when `use_gtid` is false.
    last_persisted_gtid: String,
}

impl Checkpointer {
    /// Record a compatible mid-stream layout adopt.
    ///
    /// The durable sidecar keeps fingerprints that match the resume position
    /// until [`Self::persist`] sees resume past each epoch boundary. The
    /// caller swaps the in-memory decode layout immediately; reconnect
    /// restore uses [`Self::restore_pre_adopt_if_needed`].
    ///
    /// Multiple adopts before resume crosses the first boundary append epochs
    /// while preserving the original `pre_adopt`. Epochs are kept in boundary
    /// order; a boundary already present is left unchanged (reconnect replay
    /// must not overwrite with a later `information_schema` fetch), and a
    /// boundary behind the latest known epoch is ignored (use
    /// [`Self::layout_for_replay_boundary`] instead).
    fn note_adopted_layout(
        &mut self,
        adopted: &AdoptedLayout,
        pre_adopt: AdoptedLayout,
        boundary: &BinlogPosition,
    ) {
        let epoch = LayoutEpoch {
            boundary: boundary.clone(),
            layout: adopted.clone(),
            fingerprint: adopted.layout.fingerprint(),
        };
        match self.pending_adopt.as_mut() {
            Some(pending) => {
                if pending
                    .epochs
                    .iter()
                    .any(|existing| &existing.boundary == boundary)
                {
                    // Already known (reconnect replay). Leave the recorded
                    // epoch alone — a re-fetched information_schema layout
                    // may be a later epoch and must not overwrite.
                } else if pending
                    .latest_boundary()
                    .is_some_and(|latest| boundary < latest)
                {
                    tracing::debug!(
                        dataset = %self.dataset_name,
                        boundary = %boundary,
                        "ignoring layout adopt behind an already-pending later schema-change boundary"
                    );
                } else {
                    pending.epochs.push(epoch);
                }
            }
            None => {
                self.pending_adopt = Some(PendingLayoutAdopt {
                    pre_adopt,
                    epochs: vec![epoch],
                });
            }
        }
    }

    /// Layout to use when replaying a schema-change at `boundary` while a
    /// pending chain already exists.
    ///
    /// Exact boundary hits return that epoch. Boundaries behind the latest
    /// pending epoch (e.g. reconnect replaying ALTER@A1 when only `TableMap`@T1
    /// and ALTER@A2 were recorded) must NOT trigger a fresh
    /// `information_schema` fetch — that returns today's later layout. Use
    /// [`PendingLayoutAdopt::layout_at`] instead.
    fn layout_for_replay_boundary(&self, boundary: &BinlogPosition) -> Option<&AdoptedLayout> {
        let pending = self.pending_adopt.as_ref()?;
        if let Some(exact) = pending
            .epochs
            .iter()
            .find(|epoch| &epoch.boundary == boundary)
        {
            return Some(&exact.layout);
        }
        let latest = pending.latest_boundary()?;
        (boundary < latest).then(|| pending.layout_at(boundary))
    }

    /// On reconnect, restore the decode layout that was in force at `resume`
    /// whenever a pending adopt chain still has epochs ahead of (or at) that
    /// position's required map. Returns `Some` when the live layout should be
    /// replaced.
    ///
    /// Always restores when there is a pending chain and resume is before the
    /// latest boundary — including the case where resume sits between two
    /// epochs (needs the intermediate layout, not the latest live one).
    fn restore_pre_adopt_if_needed(&self, resume: &BinlogPosition) -> Option<&AdoptedLayout> {
        let pending = self.pending_adopt.as_ref()?;
        let latest = pending.latest_boundary()?;
        // If resume is already past every pending boundary, the live layout
        // (latest adopt) is correct and durable flush will clear the chain.
        if resume >= latest {
            return None;
        }
        Some(pending.layout_at(resume))
    }

    /// Whether any pending epoch's fingerprint is eligible to become durable
    /// (`resume` has crossed at least the earliest boundary).
    fn pending_adopt_ready(&self, resume: &BinlogPosition) -> bool {
        self.pending_adopt
            .as_ref()
            .and_then(PendingLayoutAdopt::earliest_boundary)
            .is_some_and(|boundary| resume >= boundary)
    }

    /// Fold fingerprints for every epoch whose boundary is at or behind
    /// `resume` into `schema_json`, dropping those epochs. Clears the pending
    /// chain entirely once resume has crossed the latest boundary.
    ///
    /// Crossed epochs promote `pre_adopt` to the latest crossed layout so a
    /// reconnect between remaining boundaries restores the intermediate map
    /// (not the original pre-first-adopt layout). The pending chain is only
    /// mutated after the durable meta update succeeds — a failed refresh must
    /// not lose the crossed fingerprint.
    ///
    /// Returns `true` when the durable meta was updated.
    fn apply_pending_adopt_if_ready(&mut self, resume: &BinlogPosition) -> bool {
        let Some(pending) = self.pending_adopt.as_ref() else {
            return false;
        };
        let Some(fingerprint) = pending.fingerprint_at(resume).map(str::to_string) else {
            return false;
        };

        // Latest crossed epoch becomes the new `pre_adopt` baseline for any
        // epochs that remain after this apply.
        let mut promoted_pre_adopt = pending.pre_adopt.clone();
        for epoch in &pending.epochs {
            if resume < &epoch.boundary {
                break;
            }
            promoted_pre_adopt = epoch.layout.clone();
        }
        let remaining_epochs: Vec<LayoutEpoch> = pending
            .epochs
            .iter()
            .filter(|epoch| resume < &epoch.boundary)
            .cloned()
            .collect();

        let Some(json) = self.schema_json.as_deref() else {
            // No durable meta to refresh. Still advance/clear the in-memory
            // chain once resume has crossed so `pending_adopt_ready` cannot
            // stay true forever and skip source-head polls. Restart will
            // refuse `MissingCheckpointMeta` anyway.
            self.advance_or_clear_pending(promoted_pre_adopt, remaining_epochs);
            return false;
        };
        let Ok(Some(mut meta)) = CheckpointMeta::parse(json) else {
            // Corrupt / unsupported meta — same as missing: clear the chain
            // once crossed rather than wedging the event loop.
            self.advance_or_clear_pending(promoted_pre_adopt, remaining_epochs);
            return false;
        };
        if meta.source_layout_fingerprint == fingerprint {
            // Fingerprint already durable — still advance the in-memory chain
            // so reconnect restore stays consistent with resume.
            self.advance_or_clear_pending(promoted_pre_adopt, remaining_epochs);
            return false;
        }
        meta.source_layout_fingerprint = fingerprint;
        match meta.to_schema_json() {
            Ok(updated) => {
                self.schema_json = Some(updated);
                self.advance_or_clear_pending(promoted_pre_adopt, remaining_epochs);
                true
            }
            Err(e) => {
                tracing::warn!(
                    dataset = %self.dataset_name,
                    error = %e,
                    "failed to refresh checkpoint layout fingerprint after mid-stream adopt"
                );
                false
            }
        }
    }

    fn advance_or_clear_pending(
        &mut self,
        promoted_pre_adopt: AdoptedLayout,
        remaining_epochs: Vec<LayoutEpoch>,
    ) {
        if remaining_epochs.is_empty() {
            self.pending_adopt = None;
        } else if let Some(pending) = self.pending_adopt.as_mut() {
            pending.pre_adopt = promoted_pre_adopt;
            pending.epochs = remaining_epochs;
        }
    }

    /// Persist the resume position (after folding in the latest ack) when it
    /// advanced, or when a pending layout-adopt fingerprint became eligible.
    /// Sidecar failures are logged and counted, not fatal — the position
    /// re-persists on the next interval, and a crash in between only widens
    /// the idempotent replay window.
    ///
    /// Callers must not invoke this on a fatal schema-mismatch path: advancing
    /// the durable cursor past an un-adoptable DDL / `TableMap` boundary would
    /// leave a restart decoding historical row images with the current layout.
    async fn persist(&mut self, ack: &AckState, resume: &mut BinlogPosition) {
        if let Some(committed) = ack.committed() {
            advance_max(resume, committed);
        }
        let fingerprint_updated = self.apply_pending_adopt_if_ready(resume);
        // Snapshot the executed set at the same instant as the position: under
        // the in-order commit contract everything up to `resume` is in the set,
        // and nothing past it (see `AckState`).
        let gtid_set = self.use_gtid.then(|| ack.gtid_snapshot().to_string());
        // Under GTID the set is the real cursor. A failover can leave `resume`
        // frozen (new source's file ordinals lower than the persisted one), so
        // also persist whenever the executed set has grown — otherwise the
        // failover-safe checkpoint would stop advancing and the replay window
        // would grow without bound.
        let gtid_advanced = gtid_set
            .as_deref()
            .is_some_and(|set| set != self.last_persisted_gtid);
        if *resume <= self.last_persisted && !fingerprint_updated && !gtid_advanced {
            return;
        }
        let persisted = PersistedPosition {
            position: resume.clone(),
            schema_json: self.schema_json.clone(),
            gtid_set: gtid_set.clone(),
            // Stored explicitly so classification never depends on whether the
            // (possibly empty) GTID set round-trips as non-null.
            cursor_type: if self.use_gtid {
                CursorType::Gtid
            } else {
                CursorType::File
            },
        };
        match self.store.save(&persisted).await {
            Ok(()) => {
                self.metrics.inc_checkpoint_persist();
                self.metrics
                    .set_committed_position(resume.file_ordinal().unwrap_or(0), resume.pos);
                if *resume >= self.last_persisted {
                    self.last_persisted = resume.clone();
                }
                if let Some(set) = gtid_set {
                    self.last_persisted_gtid = set;
                }
            }
            Err(e) => {
                self.metrics.inc_checkpoint_persist_error();
                tracing::warn!(
                    dataset = %self.dataset_name,
                    position = %resume,
                    error = %e,
                    "failed to persist binlog position; will retry on the next checkpoint interval"
                );
            }
        }
    }
}

/// Source row-image indexes of the declared primary keys, for PK-change
/// detection on UPDATE. `column_map` is dataset-field-indexed, so PK names
/// map through the dataset schema.
fn compute_pk_source_indexes(
    schema: &SchemaRef,
    primary_keys: &[String],
    column_map: &[usize],
) -> Vec<usize> {
    primary_keys
        .iter()
        .filter_map(|pk| {
            schema
                .fields()
                .iter()
                .position(|f| f.name() == pk)
                .and_then(|field_idx| column_map.get(field_idx).copied())
        })
        .collect()
}

/// The re-validated positional mappings adopted after a source DDL.
#[derive(Clone)]
struct AdoptedLayout {
    layout: TableLayout,
    column_map: Vec<usize>,
    pk_source_indexes: Vec<usize>,
}

/// Re-fetch the source table's layout and reconcile it against the dataset
/// schema. Succeeds when every dataset column still exists on the source —
/// values keep decoding by name at their new positions. Source columns the
/// dataset doesn't declare (including ones a DDL just added) are not
/// replicated; newly-appeared ones are warned about, mirroring the Postgres
/// connector's block-mode behavior for compatible relation changes.
async fn adopt_current_layout(
    params: &ReplicationParams,
    database: &str,
    table: &str,
    schema: &SchemaRef,
    old_layout: &TableLayout,
    primary_keys: &[String],
    dataset_name: &str,
) -> Result<AdoptedLayout> {
    let mut conn = super::setup::connect(params).await?;
    let layout = super::setup::fetch_table_layout(&mut conn, database, table).await?;
    if let Err(e) = conn.disconnect().await {
        tracing::debug!(dataset = %dataset_name, error = %e, "layout refetch disconnect");
    }

    let column_map = layout.column_map(schema, database, table)?;
    let pk_source_indexes = compute_pk_source_indexes(schema, primary_keys, &column_map);

    let old_names: std::collections::HashSet<&str> =
        old_layout.columns.iter().map(|c| c.name.as_str()).collect();
    let added: Vec<&str> = layout
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .filter(|name| !old_names.contains(name))
        .collect();
    if added.is_empty() {
        tracing::info!(
            dataset = %dataset_name,
            columns = layout.columns.len(),
            "adopted the source table's new layout; all dataset columns still map"
        );
    } else {
        tracing::warn!(
            dataset = %dataset_name,
            columns = ?added,
            "source table {database}.{table} gained columns whose values are not \
             replicated. Add them to the dataset schema (and restart) to capture them"
        );
    }

    Ok(AdoptedLayout {
        layout,
        column_map,
        pk_source_indexes,
    })
}

/// Poll the source's binlog head over a lazily-maintained side connection
/// and publish the head/lag metrics. Best-effort: any failure drops the
/// connection (reopened on the next tick) and never disturbs replication.
async fn poll_source_head(
    side_conn: &mut Option<Conn>,
    params: &ReplicationParams,
    resume: &BinlogPosition,
    metrics: &MetricsCollector,
    dataset_name: &str,
) -> Option<i64> {
    let conn = match side_conn {
        Some(conn) => conn,
        None => match Conn::new(params.opts.clone()).await {
            Ok(conn) => side_conn.insert(conn),
            Err(e) => {
                tracing::debug!(
                    dataset = %dataset_name,
                    error = %e,
                    "failed to open the source-head polling connection; lag metrics deferred"
                );
                return None;
            }
        },
    };
    let head = match super::setup::fetch_head_position(conn).await {
        Ok(head) => head,
        Err(e) => {
            tracing::debug!(
                dataset = %dataset_name,
                error = %e,
                "source-head poll failed; dropping the polling connection"
            );
            *side_conn = None;
            return None;
        }
    };
    // Byte lag is exact only within one binlog file; across files the metric
    // goes absent rather than guessing.
    let lag_bytes = (head.file == resume.file).then(|| head.pos.saturating_sub(resume.pos));
    metrics.set_source_head(head.file_ordinal().unwrap_or(0), head.pos, lag_bytes);

    // Only once the stream has caught up to the head may a readiness heartbeat
    // carry a fresh "current as of now" clock — emitting one while a backlog
    // remains would mark the dataset Ready before it has applied those changes.
    if *resume < head {
        return None;
    }
    // Source-attested clock (the source's own NOW(), never a local now()), so a
    // caught-up idle source still reaches Ready under lag-based readiness.
    match super::setup::fetch_source_now_ms(conn).await {
        Ok(source_now_ms) => Some(source_now_ms),
        Err(e) => {
            tracing::debug!(
                dataset = %dataset_name,
                error = %e,
                "source clock query failed; readiness heartbeat deferred"
            );
            *side_conn = None;
            None
        }
    }
}

fn purged_position_error(resume: &BinlogPosition, dataset_name: &str) -> StreamError {
    StreamError::External(format!(
        "mysql binlog for {dataset_name}: the source no longer has binlog position {resume} \
         (binary logs were purged). Restart the dataset with \
         `mysql_replication_invalid_checkpoint_behavior: restart` to drop the saved position \
         and re-snapshot the table, or increase `binlog_expire_logs_seconds` on the source."
    ))
}

/// See `postgres_replication::client` for the WARN→DEBUG demotion rationale:
/// the first failure of an outage is loud, the rest keep log volume sublinear.
fn log_transient_reconnect(attempt: u32, dataset: &str, error: &str, retry_in_ms: u128) {
    if attempt <= 1 {
        tracing::warn!(
            dataset = %dataset,
            attempt,
            retry_in_ms = %retry_in_ms,
            error = %error,
            "binlog connection lost; reconnecting"
        );
    } else {
        tracing::debug!(
            dataset = %dataset,
            attempt,
            retry_in_ms = %retry_in_ms,
            error = %error,
            "binlog connection still down; reconnecting"
        );
    }
}

enum QueryKind {
    Begin,
    Commit,
    /// `XA START|END|PREPARE|COMMIT|ROLLBACK ...` — two-phase transactions
    /// use a different commit protocol this stream does not implement.
    Xa,
    Statement,
}

fn classify_query(statement: &str) -> QueryKind {
    let trimmed = statement.trim();
    if trimmed.eq_ignore_ascii_case("BEGIN") {
        QueryKind::Begin
    } else if trimmed.eq_ignore_ascii_case("COMMIT") {
        QueryKind::Commit
    } else if trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("xa "))
    {
        QueryKind::Xa
    } else {
        QueryKind::Statement
    }
}

#[derive(Debug, PartialEq, Eq)]
enum StatementKind {
    Truncate,
    SchemaChange(&'static str),
}

/// Detect statements that affect the subscribed table: TRUNCATE (applied as
/// a change) and schema-changing DDL (fatal — see the stream loop).
///
/// Deliberately a bounded hand-rolled tokenizer rather than sqlparser: the
/// classifier must be total over arbitrary server-wide statements, and a
/// full-parser failure on an exotic-but-relevant `ALTER` variant would
/// silently miss a schema change — worse than this prefix scan's confined
/// vocabulary.
///
/// `default_db` is the session database recorded in the query event, used
/// for unqualified table references. Identifier comparison is
/// case-insensitive (matching `MySQL`'s common `lower_case_table_names`
/// deployments; a false positive requires two tables differing only in case).
fn classify_statement(
    statement: &str,
    default_db: &str,
    database: &str,
    table: &str,
) -> Option<StatementKind> {
    let tokens = tokenize_statement(statement);
    let mut idx = 0;
    let word = |i: usize| tokens.get(i).map(Token::normalized);

    let matches_target = |db: Option<&str>, name: &str| {
        let effective_db = db.unwrap_or(default_db);
        effective_db.eq_ignore_ascii_case(database) && name.eq_ignore_ascii_case(table)
    };

    match word(idx).as_deref() {
        Some("truncate") => {
            idx += 1;
            if word(idx).as_deref() == Some("table") {
                idx += 1;
            }
            let (db, name) = parse_table_ref(&tokens, &mut idx)?;
            matches_target(db.as_deref(), &name).then_some(StatementKind::Truncate)
        }
        Some("alter") => {
            idx += 1;
            if word(idx).as_deref() != Some("table") {
                return None;
            }
            idx += 1;
            let (db, name) = parse_table_ref(&tokens, &mut idx)?;
            matches_target(db.as_deref(), &name)
                .then_some(StatementKind::SchemaChange("ALTER TABLE"))
        }
        Some("drop") => {
            idx += 1;
            match word(idx).as_deref() {
                Some("table") => {
                    idx += 1;
                    if word(idx).as_deref() == Some("if")
                        && word(idx + 1).as_deref() == Some("exists")
                    {
                        idx += 2;
                    }
                    loop {
                        let (db, name) = parse_table_ref(&tokens, &mut idx)?;
                        if matches_target(db.as_deref(), &name) {
                            return Some(StatementKind::SchemaChange("DROP TABLE"));
                        }
                        if tokens.get(idx).map(Token::normalized).as_deref() == Some(",") {
                            idx += 1;
                        } else {
                            return None;
                        }
                    }
                }
                Some("database" | "schema") => {
                    idx += 1;
                    if word(idx).as_deref() == Some("if")
                        && word(idx + 1).as_deref() == Some("exists")
                    {
                        idx += 2;
                    }
                    let name = tokens.get(idx)?.identifier()?;
                    name.eq_ignore_ascii_case(database)
                        .then_some(StatementKind::SchemaChange("DROP DATABASE"))
                }
                _ => None,
            }
        }
        Some("rename") => {
            idx += 1;
            if word(idx).as_deref() != Some("table") {
                return None;
            }
            idx += 1;
            loop {
                let (db, name) = parse_table_ref(&tokens, &mut idx)?;
                if matches_target(db.as_deref(), &name) {
                    return Some(StatementKind::SchemaChange("RENAME TABLE"));
                }
                if tokens.get(idx).map(Token::normalized).as_deref() != Some("to") {
                    return None;
                }
                idx += 1;
                let (to_db, to_name) = parse_table_ref(&tokens, &mut idx)?;
                if matches_target(to_db.as_deref(), &to_name) {
                    return Some(StatementKind::SchemaChange("RENAME TABLE"));
                }
                if tokens.get(idx).map(Token::normalized).as_deref() == Some(",") {
                    idx += 1;
                } else {
                    return None;
                }
            }
        }
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Token {
    /// Bare word — keywords and unquoted identifiers.
    Word(String),
    /// Backtick-quoted identifier (already unescaped).
    Quoted(String),
    /// Single punctuation character (`.`, `,`, …).
    Punct(char),
}

impl Token {
    fn normalized(&self) -> String {
        match self {
            Token::Word(w) => w.to_ascii_lowercase(),
            Token::Quoted(q) => q.clone(),
            Token::Punct(c) => c.to_string(),
        }
    }

    fn identifier(&self) -> Option<String> {
        match self {
            Token::Word(w) => Some(w.clone()),
            Token::Quoted(q) => Some(q.clone()),
            Token::Punct(_) => None,
        }
    }
}

fn tokenize_statement(statement: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = statement.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c == '/' {
            // Possible /* ... */ comment (MySQL prepends version comments).
            chars.next();
            if chars.peek() == Some(&'*') {
                chars.next();
                let mut prev = ' ';
                for c in chars.by_ref() {
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
            } else {
                tokens.push(Token::Punct('/'));
            }
        } else if c == '`' {
            chars.next();
            let mut ident = String::new();
            while let Some(c) = chars.next() {
                if c == '`' {
                    if chars.peek() == Some(&'`') {
                        chars.next();
                        ident.push('`');
                    } else {
                        break;
                    }
                } else {
                    ident.push(c);
                }
            }
            tokens.push(Token::Quoted(ident));
        } else if c.is_alphanumeric() || c == '_' || c == '$' {
            let mut word = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_alphanumeric() || c == '_' || c == '$' {
                    word.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            tokens.push(Token::Word(word));
        } else {
            tokens.push(Token::Punct(c));
            chars.next();
        }
    }
    tokens
}

/// Parse `ident` or `ident.ident` starting at `*idx`, advancing it past the
/// reference. Returns `(database, table)`.
fn parse_table_ref(tokens: &[Token], idx: &mut usize) -> Option<(Option<String>, String)> {
    let first = tokens.get(*idx)?.identifier()?;
    *idx += 1;
    if tokens.get(*idx) == Some(&Token::Punct('.')) {
        *idx += 1;
        let second = tokens.get(*idx)?.identifier()?;
        *idx += 1;
        Some((Some(first), second))
    } else {
        Some((None, first))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression (failover durability): under GTID the executed set is the
    /// authoritative cursor. After a failover the promoted primary's binlog
    /// file ordinals can be *lower* than the persisted position, so `resume`
    /// never advances again — but the checkpoint must still persist as the set
    /// grows, or the crash-replay window grows without bound.
    #[tokio::test]
    async fn gtid_checkpoint_persists_when_set_advances_though_position_frozen() {
        use super::super::{PersistedPosition, PositionStore, StoreError};
        use async_trait::async_trait;
        use std::sync::Mutex as StdMutex;

        #[derive(Default)]
        struct RecordingStore {
            saved: StdMutex<Option<PersistedPosition>>,
            saves: StdMutex<u32>,
        }
        #[async_trait]
        impl PositionStore for RecordingStore {
            async fn load(&self) -> std::result::Result<Option<PersistedPosition>, StoreError> {
                Ok(self.saved.lock().expect("lock").clone())
            }
            async fn save(&self, p: &PersistedPosition) -> std::result::Result<(), StoreError> {
                *self.saved.lock().expect("lock") = Some(p.clone());
                *self.saves.lock().expect("lock") += 1;
                Ok(())
            }
            async fn clear(&self) -> std::result::Result<(), StoreError> {
                *self.saved.lock().expect("lock") = None;
                Ok(())
            }
        }

        let uuid = Uuid::parse_str("3e11fa47-71ca-11e1-9e33-c80aa9429562").expect("uuid");
        // Seed = the set already persisted before the failover.
        let mut seed = GtidSet::new();
        seed.add(uuid, 5);

        let store = Arc::new(RecordingStore::default());
        // Persisted position is high (old primary, binlog.000042); the promoted
        // primary streams from a lower ordinal (binlog.000001).
        let mut checkpointer = Checkpointer {
            store: Arc::clone(&store) as Arc<dyn PositionStore>,
            schema_json: None,
            pending_adopt: None,
            dataset_name: "orders".to_string(),
            metrics: MetricsCollector::new(),
            last_persisted: BinlogPosition::new("binlog.000042", 1000),
            use_gtid: true,
            last_persisted_gtid: seed.to_string(),
        };

        let ack = AckState::new(seed);
        // Post-failover: a committed position with a LOWER file ordinal (so
        // `resume` cannot advance), plus a newly-applied txn that grows the set.
        ack.advance(&BinlogPosition::new("binlog.000001", 500));
        ack.add_gtid(uuid, 6);

        let mut resume = BinlogPosition::new("binlog.000042", 1000);
        checkpointer.persist(&ack, &mut resume).await;

        assert_eq!(
            *store.saves.lock().expect("lock"),
            1,
            "must persist when the executed set advances even though the position is frozen"
        );
        let saved = store
            .saved
            .lock()
            .expect("lock")
            .clone()
            .expect("a checkpoint was saved");
        assert_eq!(saved.cursor_type, CursorType::Gtid);
        assert_eq!(
            saved.gtid_set.as_deref(),
            Some(format!("{uuid}:5-6").as_str())
        );

        // No further advance → no redundant persist.
        checkpointer.persist(&ack, &mut resume).await;
        assert_eq!(
            *store.saves.lock().expect("lock"),
            1,
            "must not persist again when neither the position nor the set advanced"
        );
    }

    #[test]
    fn safe_advance_requires_drained_and_idle() {
        let emitted = BinlogPosition::new("binlog.000001", 500);
        let committed_behind = BinlogPosition::new("binlog.000001", 400);
        let committed_caught_up = BinlogPosition::new("binlog.000001", 500);

        // Nothing emitted yet: free to advance.
        assert!(may_safely_advance(false, None, None));
        // Transaction buffering: never advance.
        assert!(!may_safely_advance(true, None, None));
        // Emitted but not committed: hold.
        assert!(!may_safely_advance(false, Some(&emitted), None));
        assert!(!may_safely_advance(
            false,
            Some(&emitted),
            Some(&committed_behind)
        ));
        // Fully drained: advance.
        assert!(may_safely_advance(
            false,
            Some(&emitted),
            Some(&committed_caught_up)
        ));
    }

    #[test]
    fn ack_state_is_monotonic() {
        let ack = AckState::new(GtidSet::new());
        ack.advance(&BinlogPosition::new("binlog.000002", 100));
        // A late-running committer for an earlier position must not regress.
        ack.advance(&BinlogPosition::new("binlog.000001", 900));
        assert_eq!(
            ack.committed(),
            Some(BinlogPosition::new("binlog.000002", 100))
        );
    }

    #[test]
    fn pending_adopt_fingerprint_waits_for_boundary() {
        use super::super::setup::SourceColumn;
        use super::super::{CHECKPOINT_META_VERSION, CheckpointMeta};

        let col = |name: &str, ty: &str, pk: bool| SourceColumn {
            name: name.to_string(),
            column_type: ty.to_string(),
            enum_variants: None,
            set_variants: None,
            is_primary_key: pk,
        };
        let old_layout = TableLayout {
            columns: vec![col("id", "int", true), col("name", "varchar(255)", false)],
        };
        let new_layout = TableLayout {
            columns: vec![col("name", "varchar(255)", false), col("id", "int", true)],
        };
        assert_ne!(old_layout.fingerprint(), new_layout.fingerprint());

        let meta = CheckpointMeta {
            version: CHECKPOINT_META_VERSION,
            dataset_schema_json: r#"{"fields":[]}"#.to_string(),
            source_layout_fingerprint: old_layout.fingerprint(),
        };
        let schema_json = meta.to_schema_json().expect("serialize");
        let boundary = BinlogPosition::new("binlog.000001", 500);
        let mut checkpointer = Checkpointer {
            store: Arc::new(super::super::NoopPositionStore),
            schema_json: Some(schema_json),
            pending_adopt: None,
            dataset_name: "orders".to_string(),
            metrics: MetricsCollector::new(),
            last_persisted: BinlogPosition::new("binlog.000001", 100),
            use_gtid: false,
            last_persisted_gtid: String::new(),
        };

        let pre_adopt = AdoptedLayout {
            layout: old_layout.clone(),
            column_map: vec![0, 1],
            pk_source_indexes: vec![0],
        };
        let adopted = AdoptedLayout {
            layout: new_layout.clone(),
            column_map: vec![1, 0],
            pk_source_indexes: vec![1],
        };
        checkpointer.note_adopted_layout(&adopted, pre_adopt, &boundary);

        // Pre-boundary resume must not flip the durable fingerprint, and must
        // expose the pre-adopt layout for reconnect restore.
        let before = BinlogPosition::new("binlog.000001", 400);
        assert!(!checkpointer.pending_adopt_ready(&before));
        assert!(!checkpointer.apply_pending_adopt_if_ready(&before));
        let restored = checkpointer
            .restore_pre_adopt_if_needed(&before)
            .expect("pre-boundary reconnect must restore pre-adopt layout");
        assert_eq!(restored.layout.fingerprint(), old_layout.fingerprint());
        let still_old = CheckpointMeta::parse(checkpointer.schema_json.as_deref().expect("meta"))
            .expect("parse")
            .expect("v2");
        assert_eq!(
            still_old.source_layout_fingerprint,
            old_layout.fingerprint()
        );
        assert!(checkpointer.pending_adopt.is_some());

        // At/after the boundary the new fingerprint becomes eligible and
        // reconnect must NOT restore the pre-adopt layout.
        assert!(checkpointer.pending_adopt_ready(&boundary));
        assert!(
            checkpointer
                .restore_pre_adopt_if_needed(&boundary)
                .is_none()
        );
        assert!(checkpointer.apply_pending_adopt_if_ready(&boundary));
        let updated = CheckpointMeta::parse(checkpointer.schema_json.as_deref().expect("meta"))
            .expect("parse")
            .expect("v2");
        assert_eq!(updated.source_layout_fingerprint, new_layout.fingerprint());
        assert!(checkpointer.pending_adopt.is_none());
        assert!(
            checkpointer
                .restore_pre_adopt_if_needed(&boundary)
                .is_none()
        );
    }

    #[test]
    fn pending_adopt_clears_when_schema_json_missing() {
        use super::super::setup::SourceColumn;

        let col = |name: &str, ty: &str, pk: bool| SourceColumn {
            name: name.to_string(),
            column_type: ty.to_string(),
            enum_variants: None,
            set_variants: None,
            is_primary_key: pk,
        };
        let old_layout = TableLayout {
            columns: vec![col("id", "int", true), col("name", "varchar(255)", false)],
        };
        let new_layout = TableLayout {
            columns: vec![col("name", "varchar(255)", false), col("id", "int", true)],
        };
        let boundary = BinlogPosition::new("binlog.000001", 500);
        let mut checkpointer = Checkpointer {
            store: Arc::new(super::super::NoopPositionStore),
            schema_json: None,
            pending_adopt: None,
            dataset_name: "orders".to_string(),
            metrics: MetricsCollector::new(),
            last_persisted: BinlogPosition::new("binlog.000001", 100),
            use_gtid: false,
            last_persisted_gtid: String::new(),
        };
        checkpointer.note_adopted_layout(
            &AdoptedLayout {
                layout: new_layout,
                column_map: vec![1, 0],
                pk_source_indexes: vec![1],
            },
            AdoptedLayout {
                layout: old_layout,
                column_map: vec![0, 1],
                pk_source_indexes: vec![0],
            },
            &boundary,
        );
        assert!(checkpointer.pending_adopt_ready(&boundary));
        // Without durable meta, apply must still clear the chain so the event
        // loop does not skip source-head polls forever.
        assert!(!checkpointer.apply_pending_adopt_if_ready(&boundary));
        assert!(checkpointer.pending_adopt.is_none());
        assert!(!checkpointer.pending_adopt_ready(&boundary));
    }

    #[test]
    fn second_adopt_preserves_original_pre_adopt_for_reconnect() {
        use super::super::setup::SourceColumn;
        use super::super::{CHECKPOINT_META_VERSION, CheckpointMeta};

        let col = |name: &str, ty: &str, pk: bool| SourceColumn {
            name: name.to_string(),
            column_type: ty.to_string(),
            enum_variants: None,
            set_variants: None,
            is_primary_key: pk,
        };
        let l0 = TableLayout {
            columns: vec![col("id", "int", true), col("a", "int", false)],
        };
        let l1 = TableLayout {
            columns: vec![col("a", "int", false), col("id", "int", true)],
        };
        let l2 = TableLayout {
            columns: vec![
                col("a", "int", false),
                col("id", "int", true),
                col("b", "int", false),
            ],
        };

        let meta = CheckpointMeta {
            version: CHECKPOINT_META_VERSION,
            dataset_schema_json: r#"{"fields":[]}"#.to_string(),
            source_layout_fingerprint: l0.fingerprint(),
        };
        let mut checkpointer = Checkpointer {
            store: Arc::new(super::super::NoopPositionStore),
            schema_json: Some(meta.to_schema_json().expect("serialize")),
            pending_adopt: None,
            dataset_name: "orders".to_string(),
            metrics: MetricsCollector::new(),
            last_persisted: BinlogPosition::new("binlog.000001", 100),
            use_gtid: false,
            last_persisted_gtid: String::new(),
        };

        let ba = BinlogPosition::new("binlog.000001", 500);
        let bb = BinlogPosition::new("binlog.000001", 800);
        let adopted_a = AdoptedLayout {
            layout: l1.clone(),
            column_map: vec![1, 0],
            pk_source_indexes: vec![1],
        };
        let pre_a = AdoptedLayout {
            layout: l0.clone(),
            column_map: vec![0, 1],
            pk_source_indexes: vec![0],
        };
        checkpointer.note_adopted_layout(&adopted_a, pre_a, &ba);

        // Second adopt while resume is still before BA — caller's "pre_adopt"
        // is the intermediate L1 live layout; must NOT become the restore
        // baseline for positions before BA.
        let adopted_b = AdoptedLayout {
            layout: l2.clone(),
            column_map: vec![1, 0, 2],
            pk_source_indexes: vec![1],
        };
        let pre_b_wrong = AdoptedLayout {
            layout: l1.clone(),
            column_map: vec![1, 0],
            pk_source_indexes: vec![1],
        };
        checkpointer.note_adopted_layout(&adopted_b, pre_b_wrong, &bb);

        let before_a = BinlogPosition::new("binlog.000001", 400);
        let between = BinlogPosition::new("binlog.000001", 600);

        let restored_before = checkpointer
            .restore_pre_adopt_if_needed(&before_a)
            .expect("before BA must restore L0");
        assert_eq!(restored_before.layout.fingerprint(), l0.fingerprint());

        let restored_between = checkpointer
            .restore_pre_adopt_if_needed(&between)
            .expect("between BA and BB must restore L1");
        assert_eq!(restored_between.layout.fingerprint(), l1.fingerprint());

        // Durable fingerprint must not advance past BA until resume crosses BA.
        assert!(!checkpointer.apply_pending_adopt_if_ready(&before_a));
        assert!(checkpointer.apply_pending_adopt_if_ready(&between));
        let mid = CheckpointMeta::parse(checkpointer.schema_json.as_deref().expect("meta"))
            .expect("parse")
            .expect("v2");
        assert_eq!(mid.source_layout_fingerprint, l1.fingerprint());
        // L2 epoch still pending until resume crosses BB. After the partial
        // apply, reconnect at `between` must restore L1 (promoted pre_adopt),
        // not the original L0.
        assert!(checkpointer.pending_adopt.is_some());
        let restored_after_partial = checkpointer
            .restore_pre_adopt_if_needed(&between)
            .expect("after partial apply, between BA and BB must still restore L1");
        assert_eq!(
            restored_after_partial.layout.fingerprint(),
            l1.fingerprint(),
            "partial apply must promote pre_adopt to the latest crossed layout"
        );
        // Positions before BA are no longer in the pending chain's restore
        // window once BA has been crossed — restore uses promoted L1 as the
        // baseline, which is correct for any remaining pre-BB reconnect that
        // somehow reopened earlier (safe: L1 matches post-A events; pre-A
        // events are behind the durable resume floor).
        assert!(checkpointer.apply_pending_adopt_if_ready(&bb));
        let done = CheckpointMeta::parse(checkpointer.schema_json.as_deref().expect("meta"))
            .expect("parse")
            .expect("v2");
        assert_eq!(done.source_layout_fingerprint, l2.fingerprint());
        assert!(checkpointer.pending_adopt.is_none());
    }

    #[test]
    fn reconnect_replay_uses_known_boundary_not_later_epoch() {
        use super::super::setup::SourceColumn;
        use super::super::{CHECKPOINT_META_VERSION, CheckpointMeta};

        let col = |name: &str, ty: &str, pk: bool| SourceColumn {
            name: name.to_string(),
            column_type: ty.to_string(),
            enum_variants: None,
            set_variants: None,
            is_primary_key: pk,
        };
        let l0 = TableLayout {
            columns: vec![col("id", "int", true), col("a", "int", false)],
        };
        let l1 = TableLayout {
            columns: vec![col("a", "int", false), col("id", "int", true)],
        };
        let l2 = TableLayout {
            columns: vec![
                col("a", "int", false),
                col("id", "int", true),
                col("b", "int", false),
            ],
        };

        let meta = CheckpointMeta {
            version: CHECKPOINT_META_VERSION,
            dataset_schema_json: r#"{"fields":[]}"#.to_string(),
            source_layout_fingerprint: l0.fingerprint(),
        };
        let mut checkpointer = Checkpointer {
            store: Arc::new(super::super::NoopPositionStore),
            schema_json: Some(meta.to_schema_json().expect("serialize")),
            pending_adopt: None,
            dataset_name: "orders".to_string(),
            metrics: MetricsCollector::new(),
            last_persisted: BinlogPosition::new("binlog.000001", 100),
            use_gtid: false,
            last_persisted_gtid: String::new(),
        };

        let ba = BinlogPosition::new("binlog.000001", 500);
        let bb = BinlogPosition::new("binlog.000001", 800);
        checkpointer.note_adopted_layout(
            &AdoptedLayout {
                layout: l1.clone(),
                column_map: vec![1, 0],
                pk_source_indexes: vec![1],
            },
            AdoptedLayout {
                layout: l0.clone(),
                column_map: vec![0, 1],
                pk_source_indexes: vec![0],
            },
            &ba,
        );
        checkpointer.note_adopted_layout(
            &AdoptedLayout {
                layout: l2.clone(),
                column_map: vec![1, 0, 2],
                pk_source_indexes: vec![1],
            },
            AdoptedLayout {
                layout: l1.clone(),
                column_map: vec![1, 0],
                pk_source_indexes: vec![1],
            },
            &bb,
        );

        // Reconnect before BA restores L0; replaying ALTER@BA must apply the
        // known L1 epoch — not today's L2 from information_schema.
        let before_a = BinlogPosition::new("binlog.000001", 400);
        let restored = checkpointer
            .restore_pre_adopt_if_needed(&before_a)
            .expect("restore L0");
        assert_eq!(restored.layout.fingerprint(), l0.fingerprint());

        let known = checkpointer
            .layout_for_replay_boundary(&ba)
            .expect("BA must be a known pending epoch");
        assert_eq!(
            known.layout.fingerprint(),
            l1.fingerprint(),
            "reconnect replay of BA must use L1, not a later information_schema fetch"
        );

        // A spurious re-note of BA with today's L2 must not corrupt the chain.
        checkpointer.note_adopted_layout(
            &AdoptedLayout {
                layout: l2,
                column_map: vec![1, 0, 2],
                pk_source_indexes: vec![1],
            },
            AdoptedLayout {
                layout: l0,
                column_map: vec![0, 1],
                pk_source_indexes: vec![0],
            },
            &ba,
        );
        let still = checkpointer
            .layout_for_replay_boundary(&ba)
            .expect("BA still known");
        assert_eq!(
            still.layout.fingerprint(),
            l1.fingerprint(),
            "re-noting a known boundary must not overwrite with a later information_schema layout"
        );
    }

    #[test]
    fn behind_latest_unknown_boundary_uses_layout_at_not_fresh_fetch() {
        use super::super::setup::SourceColumn;
        use super::super::{CHECKPOINT_META_VERSION, CheckpointMeta};

        let col = |name: &str, ty: &str, pk: bool| SourceColumn {
            name: name.to_string(),
            column_type: ty.to_string(),
            enum_variants: None,
            set_variants: None,
            is_primary_key: pk,
        };
        let l0 = TableLayout {
            columns: vec![col("id", "int", true), col("a", "int", false)],
        };
        let l1 = TableLayout {
            columns: vec![col("a", "int", false), col("id", "int", true)],
        };
        let l2 = TableLayout {
            columns: vec![
                col("a", "int", false),
                col("id", "int", true),
                col("b", "int", false),
            ],
        };

        let meta = CheckpointMeta {
            version: CHECKPOINT_META_VERSION,
            dataset_schema_json: r#"{"fields":[]}"#.to_string(),
            source_layout_fingerprint: l0.fingerprint(),
        };
        let mut checkpointer = Checkpointer {
            store: Arc::new(super::super::NoopPositionStore),
            schema_json: Some(meta.to_schema_json().expect("serialize")),
            pending_adopt: None,
            dataset_name: "orders".to_string(),
            metrics: MetricsCollector::new(),
            last_persisted: BinlogPosition::new("binlog.000001", 100),
            use_gtid: false,
            last_persisted_gtid: String::new(),
        };

        // First change recorded only via TableMap@T1; second via ALTER@A2.
        let t1 = BinlogPosition::new("binlog.000001", 500);
        let a2 = BinlogPosition::new("binlog.000001", 800);
        let a1 = BinlogPosition::new("binlog.000001", 450); // ALTER that caused T1; ≠ T1
        checkpointer.note_adopted_layout(
            &AdoptedLayout {
                layout: l1.clone(),
                column_map: vec![1, 0],
                pk_source_indexes: vec![1],
            },
            AdoptedLayout {
                layout: l0.clone(),
                column_map: vec![0, 1],
                pk_source_indexes: vec![0],
            },
            &t1,
        );
        checkpointer.note_adopted_layout(
            &AdoptedLayout {
                layout: l2,
                column_map: vec![1, 0, 2],
                pk_source_indexes: vec![1],
            },
            AdoptedLayout {
                layout: l1.clone(),
                column_map: vec![1, 0],
                pk_source_indexes: vec![1],
            },
            &a2,
        );

        // A1 is behind latest but not an exact known boundary. Replay must use
        // layout_at(A1)=L0 (A1 < T1), never a fresh L2 fetch.
        let replay = checkpointer
            .layout_for_replay_boundary(&a1)
            .expect("behind-latest must resolve via layout_at");
        assert_eq!(
            replay.layout.fingerprint(),
            l0.fingerprint(),
            "ALTER@A1 before TableMap@T1 must keep L0 until T1, not jump to L2"
        );

        // Between T1 and A2, an unknown boundary must resolve to L1.
        let between = BinlogPosition::new("binlog.000001", 600);
        let mid = checkpointer
            .layout_for_replay_boundary(&between)
            .expect("between T1 and A2");
        assert_eq!(mid.layout.fingerprint(), l1.fingerprint());

        // Exact T1 still works.
        let at_t1 = checkpointer
            .layout_for_replay_boundary(&t1)
            .expect("exact T1");
        assert_eq!(at_t1.layout.fingerprint(), l1.fingerprint());
    }

    #[test]
    fn pk_source_indexes_follow_a_remapped_layout() {
        use arrow::datatypes::{DataType, Field, Schema};
        // Dataset (name, id); PK is `id`.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let pks = vec!["id".to_string()];

        // Original source layout: (id, name) → id at source index 0.
        assert_eq!(compute_pk_source_indexes(&schema, &pks, &[1, 0]), vec![0]);
        // Post-ALTER layout with a column inserted first: (note, id, name)
        // → id shifted to source index 1. Adoption must re-route the PK.
        assert_eq!(compute_pk_source_indexes(&schema, &pks, &[2, 1]), vec![1]);
    }

    #[test]
    fn classifies_begin_and_commit() {
        assert!(matches!(classify_query(" BEGIN "), QueryKind::Begin));
        assert!(matches!(classify_query("commit"), QueryKind::Commit));
        assert!(matches!(
            classify_query("TRUNCATE TABLE t"),
            QueryKind::Statement
        ));
    }

    #[test]
    fn truncate_statement_matches_with_and_without_qualifier() {
        for stmt in [
            "TRUNCATE TABLE orders",
            "truncate orders",
            "TRUNCATE TABLE `orders`",
            "TRUNCATE TABLE mydb.orders",
            "TRUNCATE `mydb`.`orders`",
            "/* comment */ TRUNCATE TABLE ORDERS",
        ] {
            assert_eq!(
                classify_statement(stmt, "mydb", "mydb", "orders"),
                Some(StatementKind::Truncate),
                "statement: {stmt}"
            );
        }
    }

    #[test]
    fn truncate_of_other_table_or_db_is_ignored() {
        assert_eq!(
            classify_statement("TRUNCATE TABLE customers", "mydb", "mydb", "orders"),
            None
        );
        assert_eq!(
            classify_statement("TRUNCATE otherdb.orders", "mydb", "mydb", "orders"),
            None
        );
        // Unqualified reference resolved against a different session db.
        assert_eq!(
            classify_statement("TRUNCATE orders", "otherdb", "mydb", "orders"),
            None
        );
    }

    #[test]
    fn ddl_statements_are_flagged_as_schema_changes() {
        assert_eq!(
            classify_statement(
                "ALTER TABLE orders ADD COLUMN note TEXT",
                "mydb",
                "mydb",
                "orders"
            ),
            Some(StatementKind::SchemaChange("ALTER TABLE"))
        );
        assert_eq!(
            classify_statement("DROP TABLE IF EXISTS a, mydb.orders", "x", "mydb", "orders"),
            Some(StatementKind::SchemaChange("DROP TABLE"))
        );
        assert_eq!(
            classify_statement(
                "RENAME TABLE orders TO orders_old, orders_new TO orders2",
                "mydb",
                "mydb",
                "orders"
            ),
            Some(StatementKind::SchemaChange("RENAME TABLE"))
        );
        assert_eq!(
            classify_statement("DROP DATABASE mydb", "", "mydb", "orders"),
            Some(StatementKind::SchemaChange("DROP DATABASE"))
        );
    }

    #[test]
    fn unrelated_ddl_and_dml_are_ignored() {
        for stmt in [
            "ALTER TABLE customers ADD COLUMN x INT",
            "DROP TABLE customers",
            "RENAME TABLE a TO b",
            "DROP DATABASE otherdb",
            "CREATE TABLE orders_new (id INT)",
            "INSERT INTO orders VALUES (1)",
            "OPTIMIZE TABLE orders",
        ] {
            assert_eq!(
                classify_statement(stmt, "mydb", "mydb", "orders"),
                None,
                "statement: {stmt}"
            );
        }
    }

    #[test]
    fn tokenizer_handles_backticks_and_comments() {
        let tokens = tokenize_statement("/*!80000 hint*/ TRUNCATE `my``db`.`t`");
        assert_eq!(
            tokens,
            vec![
                Token::Word("TRUNCATE".to_string()),
                Token::Quoted("my`db".to_string()),
                Token::Punct('.'),
                Token::Quoted("t".to_string()),
            ]
        );
    }
}
