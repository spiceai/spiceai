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

//! Replay a captured binlog **file** into the envelope shape the shared pump
//! delivers, for end-to-end benches and tests: the same event parser the live
//! dump uses, the same per-(transaction, table) envelopes [`super::shared`]'s
//! `deliver_commit` sends — eager materialized batches (the pump default) or
//! deferred [`MysqlChangeRows`] — without a `MySQL` server, network, or ack
//! machinery. The committer is a no-op, so consumers
//! exercise decode + apply exactly as in production while position tracking
//! stays out of the measurement.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use mysql_common::binlog::EventStreamReader;
use mysql_common::binlog::consts::BinlogVersion;
use mysql_common::binlog::events::{EventData, RowsEventData, TableMapEvent};

use super::binlog::{
    FastRowsDecoder, QueryKind, classify_query, commit_ts_ms, compute_pk_source_indexes,
};
use super::changes::{MemberLayout, MysqlChangeRows, decode_events_to_batch};
use super::metrics::MetricsCollector;
use super::setup::TableLayout;
use crate::cdc::{ChangeEnvelope, CommitChange, CommitError};

/// The binlog file magic (`0xfe 'b' 'i' 'n'`) preceding the first event.
const BINLOG_FILE_MAGIC_LEN: usize = 4;
/// The fixed binlog v4 event header, whose event-size field sits at
/// [`EVENT_SIZE_OFFSET`].
const EVENT_HEADER_LEN: usize = 19;
const EVENT_SIZE_OFFSET: usize = 9;

/// One replayed table: the dataset-side schema/keys plus the source layout the
/// live path would have fetched from `information_schema`.
pub struct ReplayTable {
    pub database: String,
    pub table: String,
    pub schema: SchemaRef,
    pub primary_keys: Vec<String>,
    pub layout: TableLayout,
    /// Build envelopes EAGERLY (materialized batches, the pump's default
    /// delivery shape) instead of deferred `MysqlChangeRows`. Matches
    /// `deliver_commit`'s eager path, including the route-cached decoder.
    pub eager: bool,
}

/// Route state for the replayed table, mirroring the pump's `Route`: the
/// current table id, its `TableMap` snapshot, and the cached fast decoder.
type ReplayRoute = (
    u64,
    Arc<TableMapEvent<'static>>,
    Option<Arc<FastRowsDecoder>>,
);

/// No-op committer: replayed envelopes have no source position to advance.
struct ReplayCommit;

#[async_trait]
impl CommitChange for ReplayCommit {
    async fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }
}

/// Parse `binlog` (a raw binlog file image, including the 4-byte magic) and
/// return the deferred-decode envelopes the shared pump would deliver for
/// `table`, in commit order. `max_bytes` (measured against the whole file
/// image) truncates the replay at the last complete transaction before the
/// cap, so benches can bound their working set.
///
/// # Errors
///
/// Returns a message when the file image is malformed or an event fails to
/// parse — the replay analog of the pump's fatal-broadcast path.
pub fn replay_binlog_envelopes(
    binlog: &[u8],
    table: &ReplayTable,
    max_bytes: Option<usize>,
) -> Result<Vec<ChangeEnvelope>, String> {
    if binlog.len() < BINLOG_FILE_MAGIC_LEN {
        return Err("binlog image shorter than the file magic".to_string());
    }
    // Truncate at the last COMPLETE event before the cap: the event parser
    // assumes whole events and panics (upstream unwrap) on a sliced tail, so
    // walk the 19-byte headers (event size lives at offset 9) to find a clean
    // boundary first.
    let cap = max_bytes.unwrap_or(binlog.len()).min(binlog.len());
    let mut limit = BINLOG_FILE_MAGIC_LEN;
    while let Some(header) = binlog.get(limit..limit + EVENT_HEADER_LEN) {
        let size = u32::from_le_bytes(
            header[EVENT_SIZE_OFFSET..EVENT_SIZE_OFFSET + 4]
                .try_into()
                .map_err(|_| "event header size field".to_string())?,
        ) as usize;
        if size < EVENT_HEADER_LEN || limit + size > cap {
            break;
        }
        limit += size;
    }

    let column_map = table
        .layout
        .column_map(&table.schema, &table.database, &table.table)
        .map_err(|e| format!("column map: {e}"))?;
    let pk_source_indexes =
        compute_pk_source_indexes(&table.schema, &table.primary_keys, &column_map);
    let member_layout = Arc::new(MemberLayout::new(
        table.layout.clone(),
        column_map,
        pk_source_indexes,
        &table.schema,
    ));
    let metrics = MetricsCollector::new();

    let mut reader = EventStreamReader::new(BinlogVersion::Version4);
    let mut io = &binlog[BINLOG_FILE_MAGIC_LEN..limit];

    let mut envelopes: Vec<ChangeEnvelope> = Vec::new();
    let mut route: Option<ReplayRoute> = None;
    // Buffered rows events of the open transaction.
    let mut txn: Vec<RowsEventData<'static>> = Vec::new();

    // A truncated trailing event (mid-write capture / byte cap) reads as
    // `Ok(None)`/`Err` and ends the replay at the last complete transaction.
    while let Ok(Some(event)) = reader.read(&mut io) {
        let event_timestamp = event.header().timestamp();
        let Ok(Some(data)) = event.read_data() else {
            continue;
        };
        match data {
            EventData::TableMapEvent(tme) => {
                if tme.database_name() == table.database && tme.table_name() == table.table {
                    let tme = tme.into_owned();
                    let decoder = table
                        .eager
                        .then(|| FastRowsDecoder::try_new(&tme).ok().map(Arc::new))
                        .flatten();
                    route = Some((tme.table_id(), Arc::new(tme), decoder));
                }
            }
            EventData::RowsEvent(rows) => {
                if let Some((table_id, _, _)) = &route
                    && rows.table_id() == *table_id
                {
                    txn.push(rows.into_owned());
                }
            }
            EventData::XidEvent(_) => {
                flush_commit(
                    &mut txn,
                    route.as_ref(),
                    table,
                    &member_layout,
                    &metrics,
                    event_timestamp,
                    &mut envelopes,
                );
            }
            EventData::QueryEvent(query) => match classify_query(&query.query()) {
                QueryKind::Commit => {
                    flush_commit(
                        &mut txn,
                        route.as_ref(),
                        table,
                        &member_layout,
                        &metrics,
                        event_timestamp,
                        &mut envelopes,
                    );
                }
                // BEGIN opens a fresh transaction; any other statement (DDL,
                // ROLLBACK, …) invalidates the buffered rows either way.
                _ => txn.clear(),
            },
            _ => {}
        }
        if io.is_empty() {
            break;
        }
    }
    Ok(envelopes)
}

fn flush_commit(
    txn: &mut Vec<RowsEventData<'static>>,
    route: Option<&ReplayRoute>,
    table: &ReplayTable,
    member_layout: &Arc<MemberLayout>,
    metrics: &Arc<MetricsCollector>,
    event_timestamp: u32,
    envelopes: &mut Vec<ChangeEnvelope>,
) {
    let events = std::mem::take(txn);
    if events.is_empty() {
        return;
    }
    let Some((_, tme, decoder)) = route else {
        return;
    };
    if table.eager {
        // The pump's default delivery shape: decode at replay time and ship a
        // materialized batch, so consumers measure pure apply cost.
        let Ok(batch) = decode_events_to_batch(
            &table.primary_keys,
            member_layout,
            tme,
            decoder.as_deref(),
            &events,
            commit_ts_ms(event_timestamp),
            metrics,
        ) else {
            return;
        };
        envelopes.push(ChangeEnvelope::new(Box::new(ReplayCommit), batch, false));
        return;
    }
    let rows = MysqlChangeRows::new(
        table.primary_keys.clone(),
        Arc::clone(member_layout),
        Arc::clone(tme),
        events,
        commit_ts_ms(event_timestamp),
        Arc::clone(metrics),
    );
    envelopes.push(ChangeEnvelope::new_from_rows(
        Box::new(ReplayCommit),
        Box::new(rows),
        false,
    ));
}
