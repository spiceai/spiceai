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

//! Deferred (off-the-pump) decode + Arrow build for the shared binlog dump.
//!
//! Mirrors [`crate::postgres_replication::changes::PgChangeRows`]: the shared
//! pump ([`super::shared`]) must not pay the O(rows × columns) tuple decode and
//! Arrow-typing cost on its single serialization point, or one large/slow table
//! would stall every other member behind it. So at each rows event the pump
//! only buffers the **owned** wire payload (`RowsEventData<'static>`) plus
//! `Arc` snapshots of the table-map columns and the member's decode layout, and
//! hands them to this [`MysqlChangeRows`]. [`ChangeRows::build`] then runs the
//! decode + [`build_change_batch`] later, on the per-dataset consumer thread.
//!
//! Two consequences, both intended:
//!   - shared-pump CPU stays ~flat as members grow, and
//!   - a row-decode failure surfaces as a per-dataset [`StreamError`] from
//!     `build` (via [`ChangeBatchError::DeferredBuild`]) rather than a pump-side
//!     `member_fatal`, so one malformed table faults only its own stream.

use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit, SchemaRef};
use mysql_async::binlog::events::{RowsEventData, TableMapEvent};

use super::binlog::{TableMapRowDecoder, buffer_rows_event, buffer_rows_event_fast};
use super::metrics::MetricsCollector;
use super::rows::{TransactionBuffer, build_change_batch};
use super::setup::TableLayout;
use crate::cdc::{ChangeBatch, ChangeBatchError, ChangeRows};

/// The positional decode layout of one member, snapshotted immutably behind an
/// `Arc`. The shared pump clones the current `Arc` into a route at
/// `TableMapEvent` install time and swaps in a fresh `Arc` on a compatible
/// mid-stream ALTER, so the snapshot handed to a [`MysqlChangeRows`] is always
/// the layout valid *when its rows were written* — never a reference the pump
/// keeps mutating.
pub(super) struct MemberLayout {
    pub(super) layout: TableLayout,
    /// Dataset field index → source row-image index.
    pub(super) column_map: Vec<usize>,
    /// Source row-image indexes of the declared primary keys.
    pub(super) pk_source_indexes: Vec<usize>,
}

/// Deferred [`ChangeRows`] for one member's rows within one committed source
/// transaction. Carries owned wire payloads; the decode runs in [`Self::build`].
pub(crate) struct MysqlChangeRows {
    schema: SchemaRef,
    primary_keys: Vec<String>,
    /// Decode-time layout snapshot (see [`MemberLayout`]).
    layout: Arc<MemberLayout>,
    /// Table-map columns valid for `events`, snapshotted at route install.
    tme: Arc<TableMapEvent<'static>>,
    /// Owned row-event payloads buffered by the pump for this table+commit.
    events: Vec<RowsEventData<'static>>,
    source_commit_ts_ms: Option<i64>,
    /// Per-row `inc_insert/inc_update/inc_delete` are recorded here in
    /// [`Self::build`] — NOT on the pump. Unlike Postgres (1 pgoutput message =
    /// 1 row, countable by a tag-peek), a `MySQL` rows event holds N rows whose
    /// count is only known by decoding, so the per-row op metrics move to the
    /// consumer. `inc_transaction` / `record_watermark` stay on the pump.
    metrics: Arc<MetricsCollector>,
    /// Precomputed decode-free metadata (see [`Self::new`]).
    row_hint: usize,
    byte_len: usize,
}

impl MysqlChangeRows {
    pub(super) fn new(
        schema: SchemaRef,
        primary_keys: Vec<String>,
        layout: Arc<MemberLayout>,
        tme: Arc<TableMapEvent<'static>>,
        events: Vec<RowsEventData<'static>>,
        source_commit_ts_ms: Option<i64>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        // Both metadata figures are computed WITHOUT decoding, from the buffered
        // wire size (`rows_data()` is a byte-slice accessor, no row parse).
        let wire_bytes: usize = events.iter().map(|e| e.rows_data().len()).sum();
        let per_row_fixed: usize = schema
            .fields()
            .iter()
            .map(|f| arrow_fixed_width(f.data_type()))
            .sum();
        // Row count can't be known without decoding a MySQL rows event, so this
        // is an estimate: wire bytes over a per-row floor, never below one row
        // per event. Over/under-estimating only affects builder pre-allocation;
        // the exact-zero case is served by `is_empty` (events are never empty).
        let row_hint = wire_bytes.div_ceil(per_row_fixed.max(1)).max(events.len());
        // Coalescing byte estimate: raw wire bytes floored at the fixed-width
        // Arrow footprint (NULL/short columns under-count the eventual Arrow
        // allocation), matching `PgChangeRows`.
        let byte_len = wire_bytes.max(row_hint.saturating_mul(per_row_fixed));
        Self {
            schema,
            primary_keys,
            layout,
            tme,
            events,
            source_commit_ts_ms,
            metrics,
            row_hint,
            byte_len,
        }
    }
}

impl ChangeRows for MysqlChangeRows {
    fn is_empty(&self) -> bool {
        // Exact: a MySQL rows event always carries at least one row, so no
        // buffered events ⟺ no output rows.
        self.events.is_empty()
    }

    fn num_rows_hint(&self) -> usize {
        self.row_hint
    }

    fn encoded_len(&self) -> usize {
        self.byte_len
    }

    fn source_commit_ts_ms(&self) -> Option<i64> {
        self.source_commit_ts_ms
    }

    fn is_heartbeat(&self) -> bool {
        // Row batches always carry rows; readiness heartbeats are emitted
        // separately as zero-row envelopes.
        false
    }

    fn build(self: Box<Self>) -> Result<ChangeBatch, ChangeBatchError> {
        // Decode every buffered rows event into the transaction buffer, then
        // build one Arrow batch. The prepared decoder reuses table-map metadata;
        // fall back to the `buffer_rows_event` walk when it cannot be built for
        // this table map. Both paths record per-row op metrics as they decode.
        let mut buffer = TransactionBuffer::new();
        let decoder = match TableMapRowDecoder::try_new(&self.tme) {
            Ok(decoder) => Some(decoder),
            Err(e) => {
                warn_decoder_fallback_once(&self.tme, &e);
                None
            }
        };
        for event in &self.events {
            match &decoder {
                Some(decoder) => buffer_rows_event_fast(
                    event,
                    decoder,
                    &self.layout.layout,
                    &self.layout.pk_source_indexes,
                    &mut buffer,
                    &self.metrics,
                ),
                None => buffer_rows_event(
                    event,
                    &self.tme,
                    &self.layout.layout,
                    &self.layout.pk_source_indexes,
                    &mut buffer,
                    &self.metrics,
                ),
            }
            .map_err(|e| ChangeBatchError::DeferredBuild {
                message: e.to_string(),
            })?;
        }
        build_change_batch(
            &self.schema,
            &self.primary_keys,
            &self.layout.column_map,
            &buffer.changes,
        )
        .map(|b| b.with_source_commit_ts_ms(self.source_commit_ts_ms))
        .map_err(|e| ChangeBatchError::DeferredBuild {
            message: e.to_string(),
        })
    }
}

/// Warn — once per (database, table) for the process lifetime — that the
/// prepared row decoder cannot be built and every transaction on this table
/// is decoding through the ~7× slower `mysql_common` walk. `build` runs per
/// committed transaction, so warning unconditionally would repeat this
/// hundreds of times per second on a busy table; a sustained fallback is a
/// per-table condition (its table map doesn't change between commits), so
/// one line carries all the signal.
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

/// Fixed per-value Arrow byte width for a data type, or 0 for variable-width
/// types (whose bytes are already reflected in the buffered wire size). Used
/// only to floor the coalescing byte estimate at the real Arrow footprint;
/// mirrors `postgres_replication::changes::arrow_fixed_width`.
fn arrow_fixed_width(data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime)
        | DataType::Timestamp(_, _) => 8,
        DataType::Decimal128(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => 16,
        DataType::Decimal256(_, _) => 32,
        DataType::FixedSizeBinary(len) => usize::try_from(*len).unwrap_or(0),
        _ => 0,
    }
}
