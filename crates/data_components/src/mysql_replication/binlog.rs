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

use super::config::{BinlogPosition, ReplicationParams};
use super::metrics::MetricsCollector;
use super::rows::{TransactionBuffer, build_change_batch, normalize_binlog_value, truncate_change};
use super::setup::TableLayout;
use super::{Error, PersistedPosition, PositionStore, Result};
use crate::cdc::{ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError};

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
    /// Serialized dataset schema persisted alongside each checkpoint for
    /// drift detection on resume.
    pub schema_json: Option<String>,
    pub metrics: Arc<MetricsCollector>,
}

/// Highest transaction-end position whose envelope committer has run.
/// Shared between the stream (reader) and every emitted envelope's
/// committer (writers).
#[derive(Default)]
struct AckState {
    committed: Mutex<Option<BinlogPosition>>,
}

impl AckState {
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
}

/// `CommitChange` impl that advances the shared ack position. Persistence to
/// the sidecar happens on the stream's periodic checkpoint, so commits stay
/// as cheap as the Postgres LSN bump.
struct PositionCommitter {
    ack: Arc<AckState>,
    position: BinlogPosition,
}

#[async_trait]
impl CommitChange for PositionCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.ack.advance(&self.position);
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
        let ack = Arc::new(AckState::default());
        // Monotonic resume/ack position: what we reconnect from and persist.
        let mut resume = start.clone();
        let mut checkpointer = Checkpointer {
            store: position_store,
            schema_json,
            dataset_name: dataset_name.clone(),
            metrics: Arc::clone(&metrics),
            last_persisted: start,
        };
        let mut last_persist_at = Instant::now();
        let mut last_emitted: Option<BinlogPosition> = None;
        // Lazily-opened side connection for the periodic source-head poll
        // (`SHOW BINARY LOG STATUS`) behind the lag metrics; dropped and
        // reopened on error, never load-bearing for replication itself.
        let mut side_conn: Option<Conn> = None;
        let mut backoff = super::resilience::StreamBackoff::default_for_stream();
        let mut reconnect_attempts: u32 = 0;

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

            let mut stream = match open_binlog_stream(&params, &resume, &dataset_name).await {
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
            // table-map events, so nothing carries over.
            let mut current_file = resume.file.clone();
            let mut txn: Option<TransactionBuffer> = None;

            'recv: loop {
                if crate::cdc::shutdown_epoch() != shutdown_epoch {
                    // Release the dump thread now rather than at process
                    // exit; the shutdown drain phase can take tens of
                    // seconds. Checked per event, so the bound is one
                    // heartbeat interval on a quiet source.
                    if let Err(e) = stream.close().await {
                        tracing::debug!(dataset = %dataset_name, error = %e, "binlog stream close during shutdown");
                    }
                    checkpointer.persist(&ack, &mut resume).await;
                    tracing::info!(dataset = %dataset_name, "runtime shutdown; released binlog connection");
                    break 'reconnect;
                }

                let Some(event) = stream.next().await else {
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
                            match adopt_current_layout(
                                &params, &database, &table, &schema, &layout, &primary_keys,
                                &dataset_name,
                            ).await {
                                Ok(adopted)
                                    if adopted.layout.columns.len() as u64
                                        == tme.columns_count() =>
                                {
                                    layout = adopted.layout;
                                    column_map = adopted.column_map;
                                    pk_source_indexes = adopted.pk_source_indexes;
                                }
                                outcome => {
                                    metrics.inc_schema_mismatch_error();
                                    checkpointer.persist(&ack, &mut resume).await;
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
                                         `mysql_replication_invalid_position_behavior: rebootstrap`.",
                                        tme.columns_count(),
                                        layout.columns.len()
                                    )))?;
                                    unreachable!();
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
                                            Box::new(PositionCommitter { ack: Arc::clone(&ack), position: commit_pos.clone() }),
                                            batch,
                                            false,
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
                                        match adopt_current_layout(
                                            &params, &database, &table, &schema, &layout,
                                            &primary_keys, &dataset_name,
                                        ).await {
                                            Ok(adopted) => {
                                                layout = adopted.layout;
                                                column_map = adopted.column_map;
                                                pk_source_indexes = adopted.pk_source_indexes;
                                            }
                                            Err(e) => {
                                                metrics.inc_schema_mismatch_error();
                                                checkpointer.persist(&ack, &mut resume).await;
                                                Err(StreamError::External(format!(
                                                    "mysql binlog for {dataset_name}: ALTER TABLE on source table \
                                                     {database}.{table} (statement: {statement}) cannot be adopted \
                                                     mid-stream: {e}. Update the dataset schema to match the new \
                                                     table definition, or re-bootstrap by setting \
                                                     `mysql_replication_invalid_position_behavior: rebootstrap`."
                                                )))?;
                                                unreachable!();
                                            }
                                        }
                                    }
                                    Some(StatementKind::SchemaChange(verb)) => {
                                        metrics.inc_schema_mismatch_error();
                                        checkpointer.persist(&ack, &mut resume).await;
                                        Err(StreamError::External(format!(
                                            "mysql binlog for {dataset_name}: {verb} detected on source table \
                                             {database}.{table} (statement: {statement}). The subscribed table \
                                             no longer exists under this name — fix the source (or the dataset) \
                                             and re-bootstrap by setting \
                                             `mysql_replication_invalid_position_behavior: rebootstrap`."
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
                    // transaction.
                    Some(EventData::GtidEvent(_) | EventData::AnonymousGtidEvent(_)) => {
                        txn = Some(TransactionBuffer::new());
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
                    if let Some(envelope) = commit_transaction(
                        &mut txn, &commit_pos, event_timestamp, &schema, &primary_keys,
                        &column_map, &ack, &metrics, &dataset_name,
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

                if last_persist_at.elapsed() >= params.checkpoint_interval {
                    checkpointer.persist(&ack, &mut resume).await;
                    poll_source_head(&mut side_conn, &params, &resume, &metrics, &dataset_name).await;
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
    if let Err(e) = mysql_async::prelude::Queryable::query_drop(
        &mut conn,
        format!(
            "SET @master_heartbeat_period = {heartbeat_nanos}, \
             @source_heartbeat_period = {heartbeat_nanos}"
        ),
    )
    .await
    {
        tracing::debug!(dataset = %dataset_name, error = %e, "failed to set the heartbeat period");
    }

    let pos_u32 = u32::try_from(resume.pos).unwrap_or(u32::MAX);
    conn.get_binlog_stream(
        BinlogStreamRequest::new(params.server_id)
            .with_filename(resume.file.as_bytes())
            .with_pos(u64::from(pos_u32)),
    )
    .await
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

    Ok(Some(ChangeEnvelope::new(
        Box::new(PositionCommitter {
            ack: Arc::clone(ack),
            position: commit_pos.clone(),
        }),
        batch,
        false,
    )))
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

/// Owns the durable half of the ack pipeline: folds committer acks into the
/// resume position and writes it to the position store when it advanced.
struct Checkpointer {
    store: Arc<dyn PositionStore>,
    schema_json: Option<String>,
    dataset_name: String,
    metrics: Arc<MetricsCollector>,
    last_persisted: BinlogPosition,
}

impl Checkpointer {
    /// Persist the resume position (after folding in the latest ack) when it
    /// advanced. Sidecar failures are logged and counted, not fatal — the
    /// position re-persists on the next interval, and a crash in between only
    /// widens the idempotent replay window.
    async fn persist(&mut self, ack: &AckState, resume: &mut BinlogPosition) {
        if let Some(committed) = ack.committed() {
            advance_max(resume, committed);
        }
        if *resume <= self.last_persisted {
            return;
        }
        let persisted = PersistedPosition {
            position: resume.clone(),
            schema_json: self.schema_json.clone(),
        };
        match self.store.save(&persisted).await {
            Ok(()) => {
                self.metrics.inc_checkpoint_persist();
                self.metrics
                    .set_committed_position(resume.file_ordinal().unwrap_or(0), resume.pos);
                self.last_persisted = resume.clone();
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
) {
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
                return;
            }
        },
    };
    match super::setup::fetch_head_position(conn).await {
        Ok(head) => {
            // Byte lag is exact only within one binlog file; across files
            // the metric goes absent rather than guessing.
            let lag_bytes = (head.file == resume.file).then(|| head.pos.saturating_sub(resume.pos));
            metrics.set_source_head(head.file_ordinal().unwrap_or(0), head.pos, lag_bytes);
        }
        Err(e) => {
            tracing::debug!(
                dataset = %dataset_name,
                error = %e,
                "source-head poll failed; dropping the polling connection"
            );
            *side_conn = None;
        }
    }
}

fn purged_position_error(resume: &BinlogPosition, dataset_name: &str) -> StreamError {
    StreamError::External(format!(
        "mysql binlog for {dataset_name}: the source no longer has binlog position {resume} \
         (binary logs were purged). Restart the dataset with \
         `mysql_replication_invalid_position_behavior: rebootstrap` to drop the saved position \
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
        let ack = AckState::default();
        ack.advance(&BinlogPosition::new("binlog.000002", 100));
        // A late-running committer for an earlier position must not regress.
        ack.advance(&BinlogPosition::new("binlog.000001", 900));
        assert_eq!(
            ack.committed(),
            Some(BinlogPosition::new("binlog.000002", 100))
        );
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
