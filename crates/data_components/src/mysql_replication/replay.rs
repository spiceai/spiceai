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
//! dump uses, the same per-(transaction, table) materialized envelopes
//! [`super::shared`]'s `deliver_commit` sends — without a `MySQL` server,
//! network, or ack machinery. The committer is a no-op, so consumers exercise
//! the apply path exactly as in production while position tracking stays out
//! of the measurement.

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use mysql_common::binlog::EventStreamReader;
use mysql_common::binlog::consts::BinlogVersion;
use mysql_common::binlog::events::{EventData, RowsEventData, TableMapEvent};

use super::binlog::{QueryKind, classify_query, commit_ts_ms, compute_pk_source_indexes};
use super::changes::{MemberLayout, decode_events_to_batch};
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
}

/// No-op committer: replayed envelopes have no source position to advance.
struct ReplayCommit;

#[async_trait]
impl CommitChange for ReplayCommit {
    async fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }
}

/// Parse `binlog` (a raw binlog file image, including the 4-byte magic) and
/// return the materialized envelopes the shared pump would deliver for
/// `table`, in commit order. `max_bytes` (measured against the whole file
/// image) truncates the replay at the last complete transaction before the
/// cap, so benches can bound their working set.
///
/// # Errors
///
/// Returns a message when the file image is malformed or an event fails to
/// parse or decode — the replay analog of the pump's member-fatal path.
pub fn replay_binlog_envelopes(
    binlog: &[u8],
    table: &ReplayTable,
    max_bytes: usize,
) -> Result<Vec<ChangeEnvelope>, String> {
    if binlog.len() < BINLOG_FILE_MAGIC_LEN {
        return Err("binlog image shorter than the file magic".to_string());
    }
    // Truncate at the last COMPLETE event before the cap: the event parser
    // assumes whole events and panics (upstream unwrap) on a sliced tail, so
    // walk the event headers (event size lives at offset 9) to find a clean
    // boundary first.
    let cap = max_bytes.min(binlog.len());
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
    let member_layout = MemberLayout {
        layout: table.layout.clone(),
        column_map,
        pk_source_indexes,
    };
    let metrics = MetricsCollector::new();

    let mut reader = EventStreamReader::new(BinlogVersion::Version4);
    let mut io = &binlog[BINLOG_FILE_MAGIC_LEN..limit];

    let mut envelopes: Vec<ChangeEnvelope> = Vec::new();
    // Route state, mirroring the pump: the current TableMap snapshot for the
    // replayed table and the buffered rows events of the open transaction.
    let mut route: Option<(u64, TableMapEvent<'static>)> = None;
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
                    route = Some((tme.table_id(), tme));
                }
            }
            EventData::RowsEvent(rows) => {
                if let Some((table_id, _)) = &route
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
                )?;
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
                    )?;
                }
                // BEGIN opens a fresh transaction; any other statement (DDL,
                // ROLLBACK, …) invalidates the buffered rows either way.
                _ => txn.clear(),
            },
            _ => {}
        }
    }
    Ok(envelopes)
}

/// Decode the open transaction's buffered events for the replayed table into a
/// materialized envelope, exactly as the pump's `deliver_commit` does.
fn flush_commit(
    txn: &mut Vec<RowsEventData<'static>>,
    route: Option<&(u64, TableMapEvent<'static>)>,
    table: &ReplayTable,
    member_layout: &MemberLayout,
    metrics: &MetricsCollector,
    event_timestamp: u32,
    envelopes: &mut Vec<ChangeEnvelope>,
) -> Result<(), String> {
    let events = std::mem::take(txn);
    if events.is_empty() {
        return Ok(());
    }
    let Some((_, tme)) = route else {
        return Ok(());
    };
    let batch = decode_events_to_batch(
        &table.schema,
        &table.primary_keys,
        member_layout,
        tme,
        &events,
        commit_ts_ms(event_timestamp),
        metrics,
    )
    .map_err(|e| format!("decode committed transaction: {e}"))?;
    envelopes.push(ChangeEnvelope::new(Box::new(ReplayCommit), batch, false));
    Ok(())
}
