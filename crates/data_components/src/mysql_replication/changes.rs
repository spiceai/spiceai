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

//! Decode + Arrow build for the shared binlog dump's committed transactions.
//!
//! The pump decodes each commit's buffered rows events into a ready
//! [`ChangeBatch`] before delivery ([`super::shared`]'s `deliver_commit`), the
//! shape [`crate::cdc::ChangeEnvelope::new`] carries. A `MySQL` rows-event
//! decode dominates the cost of a change build — 73–81 % of the CDC apply loop
//! on SF1000 CH-benCH runs — so decoding here pipelines it with the consumer's
//! accelerator writes instead of serializing the two, while the pump's
//! must-deliver backpressure still paces delivery by the slowest member.
//! (Postgres defers its cheaper per-message pgoutput decode to the per-dataset
//! consumer, [`crate::postgres_replication::changes::PgChangeRows`].)
//!
//! A row-decode failure is member-fatal (`member_fatal`), so one malformed
//! table faults only its own dataset's stream.

use arrow::datatypes::SchemaRef;
use mysql_async::binlog::events::{RowsEventData, TableMapEvent};

use super::binlog::{TableMapRowDecoder, buffer_rows_event, buffer_rows_event_fast};
use super::metrics::MetricsCollector;
use super::rows::{TransactionBuffer, build_change_batch};
use super::setup::TableLayout;
use crate::cdc::ChangeBatch;

/// The positional decode layout of one member, snapshotted immutably behind an
/// `Arc`. The shared pump clones the current `Arc` into a route at
/// `TableMapEvent` install time and swaps in a fresh `Arc` on a compatible
/// mid-stream ALTER, so the snapshot a commit is decoded against is always the
/// layout valid *when its rows were written* — never a reference the pump
/// keeps mutating.
pub(super) struct MemberLayout {
    pub(super) layout: TableLayout,
    /// Dataset field index → source row-image index.
    pub(super) column_map: Vec<usize>,
    /// Source row-image indexes of the declared primary keys.
    pub(super) pk_source_indexes: Vec<usize>,
}

/// Decode one commit's buffered rows events for one member into a
/// [`ChangeBatch`]: every event through [`buffer_rows_event`] (which records
/// the per-row op metrics as it decodes), then one Arrow build.
///
/// # Errors
///
/// Returns the decode or batch-build error; the pump treats it as
/// member-fatal so only this dataset's stream faults.
pub(super) fn decode_events_to_batch(
    schema: &SchemaRef,
    primary_keys: &[String],
    layout: &MemberLayout,
    tme: &TableMapEvent<'static>,
    events: &[RowsEventData<'static>],
    source_commit_ts_ms: Option<i64>,
    metrics: &MetricsCollector,
) -> super::Result<ChangeBatch> {
    let mut buffer = TransactionBuffer::new();
    // Prefer the prepared value-only decoder (per-`TableMapEvent` cached column
    // schema); fall back to the `mysql_common` walk when it cannot be built for
    // this table map. Both paths record per-row op metrics as they decode.
    let decoder = match TableMapRowDecoder::try_new(tme) {
        Ok(decoder) => Some(decoder),
        Err(e) => {
            warn_decoder_fallback_once(tme, &e);
            None
        }
    };
    for event in events {
        match &decoder {
            Some(decoder) => buffer_rows_event_fast(
                event,
                decoder,
                &layout.layout,
                &layout.pk_source_indexes,
                &mut buffer,
                metrics,
            ),
            None => buffer_rows_event(
                event,
                tme,
                &layout.layout,
                &layout.pk_source_indexes,
                &mut buffer,
                metrics,
            ),
        }?;
    }
    build_change_batch(schema, primary_keys, &layout.column_map, &buffer.changes)
        .map(|b| b.with_source_commit_ts_ms(source_commit_ts_ms))
}

/// Warn — once per (database, table) for the process lifetime — that the
/// prepared row decoder cannot be built and every transaction on this table
/// is decoding through the ~7× slower `mysql_common` walk. Decode runs per
/// committed transaction, so warning unconditionally would repeat this
/// hundreds of times per second on a busy table; a sustained fallback is a
/// per-table condition (its table map doesn't change between commits), so one
/// line carries all the signal.
fn warn_decoder_fallback_once(tme: &TableMapEvent<'_>, error: &super::Error) {
    static WARNED: std::sync::LazyLock<
        parking_lot::Mutex<std::collections::HashSet<(String, String)>>,
    > = std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashSet::new()));
    let key = (
        tme.database_name().to_string(),
        tme.table_name().to_string(),
    );
    if WARNED.lock().insert(key) {
        tracing::warn!(
            database = %tme.database_name(),
            table = %tme.table_name(),
            error = %error,
            "prepared row decoder unavailable for this table; MySQL CDC changes keep applying correctly through the slower mysql_common row walk (logged once per table)"
        );
    }
}
