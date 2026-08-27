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

//! Shared binlog-dump primitives used by the multiplexed pump in
//! [`super::shared`], which is the sole `MySQL` CDC streaming engine.
//!
//! `MySQL`'s `COM_BINLOG_DUMP`/`COM_BINLOG_DUMP_GTID` is server-wide with no
//! server-side table filter, so every `refresh_mode: changes` dataset on a
//! connection is coalesced onto one dump ([`super::shared`]); this module holds
//! the pieces that pump needs and owns no stream loop of its own:
//!
//!   - [`open_binlog_stream`] to start a dump at a file+offset resume or, when
//!     the source is `gtid_mode = ON`, GTID auto-positioning from an executed
//!     set (failover-safe),
//!   - [`buffer_rows_event`] to decode a rows event into a [`TransactionBuffer`],
//!   - [`readiness_heartbeat`] / [`record_watermark`] / [`commit_ts_ms`] for
//!     lag-based readiness and freshness metrics,
//!   - [`adopt_current_layout`] + [`compute_pk_source_indexes`] for compatible
//!     mid-stream `ALTER` adoption, and [`layout_event_mismatch`] to check an
//!     adopted layout against the row images it will actually decode,
//!   - [`classify_query`] / [`classify_statement`] for the transaction and DDL
//!     boundaries in the event stream.
//!
//! # Ack model
//!
//! `MySQL` keeps no server-side cursor, so the ack is Spice's own persisted
//! [`BinlogPosition`] (plus an executed [`super::GtidSet`] when GTID-positioning),
//! held per member and folded into the shared dump's resume by [`super::shared`].
//! A crash replays at most `checkpoint_interval` of already-applied changes,
//! which the accelerator's PK upsert absorbs idempotently (at-least-once).

use std::time::{Duration, SystemTime};

use arrow::datatypes::SchemaRef;
use bitvec::slice::BitSlice;
use mysql_async::binlog::events::{OptionalMetaExtractor, RowsEventData, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::consts::ColumnType;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, Value};
use mysql_common::io::ParseBuf;

use super::config::{BinlogPosition, ReplicationParams};
use super::gtid::GtidSet;
use super::metrics::MetricsCollector;
use super::rows::{TransactionBuffer, normalize_binlog_value};
use super::setup::TableLayout;
use super::{Error, Result};
use crate::cdc::{ChangeEnvelope, StreamError, build_heartbeat_envelope};

/// Binlog events start at offset 4 (after the magic header); positions below
/// that (fake rotates and heartbeats report 0) are not resumable.
pub(super) const MIN_VALID_EVENT_POS: u64 = 4;

/// Floor, in seconds, for how long the source will hold unread dump data before
/// aborting the connection. Applied to the dump session because the server
/// default is 60s.
///
/// The dump is one-way once started, so the server never waits for us to say
/// anything — `net_write_timeout` fires purely on data we have not read. Every
/// `refresh_mode: changes` dataset on a connection shares one dump, and the
/// pump must-delivers each envelope into a bounded per-dataset channel, so a
/// single dataset whose apply loop stalls (an in-memory-tier spill, for
/// instance) stops the socket being drained for all of them. At the default the
/// source then aborts the dump and every member resumes from its last acked
/// position while already far behind, which deepens lag and invites the next
/// abort.
///
/// 180s clears the worst apply cycle observed on CH-benCHmark SF1000 (~100s)
/// with ~1.8x margin. This is empirical headroom, not a bound: the apply tail
/// is not provably bounded (`cdc_max_coalesced_envelopes`/`_bytes` bound the
/// burst, not how long a spill takes), so a genuinely wedged apply still
/// reaches the timeout — it stays visible as the member send stall warning and
/// its `replication_member_send_stalled_seconds_total` counter.
///
/// A floor rather than an assignment: an operator who already raised
/// `net_write_timeout` past this — the manual workaround for exactly this
/// symptom — must not have it lowered by connecting a newer runtime, so the
/// floor is applied only to a session that inherits something lower.
const DUMP_NET_WRITE_TIMEOUT_SECS: u32 = 180;

/// What the operator loses whenever the dump session keeps a `net_write_timeout`
/// below the floor — carried by the statement that raises it, and by the read
/// that decides whether to raise it.
const NET_WRITE_TIMEOUT_NOT_RAISED: &str = "the source can still abort the shared binlog connection when one dataset's apply loop stalls, delaying changes for every changes-mode dataset on it. Grant the replication user permission to set session variables, or raise the source's net_write_timeout. See: https://spiceai.org/docs/components/data-connectors/mysql";

/// One statement issued on the dump connection before `COM_BINLOG_DUMP`.
struct PreDumpStatement {
    sql: String,
    /// What the user loses if the server rejects this statement, when that is
    /// worth saying. The heartbeat spellings are deliberately tried in pairs
    /// and one of them is unknown on any given server version, so those carry
    /// nothing and a rejection stays at debug.
    rejection_warning: Option<&'static str>,
}

/// The session setup a dump connection needs, in the order it is issued.
///
/// `inherited_net_write_timeout` is what this connection already carries, as read
/// by [`inherited_net_write_timeout`], or `None` when the server did not answer
/// with a value.
///
/// Split out from [`open_binlog_stream`] so the statements — in particular
/// which of them address a *user* variable versus a *system* one, and which
/// values they assign — are assertable without a `MySQL` server.
fn pre_dump_session_statements(
    checkpoint_interval: Duration,
    inherited_net_write_timeout: Option<u32>,
) -> Vec<PreDumpStatement> {
    // Ask the source to send heartbeat events while idle so the stream can
    // detect dead connections and advance its checkpoint. Half the
    // checkpoint interval (min 500ms) keeps idle persists within ~1.5×
    // the interval. The session variable is in nanoseconds; MySQL 8.4
    // renamed the replica-facing vocabulary, so set both spellings (unknown
    // user variables are inert).
    let heartbeat_nanos = (checkpoint_interval / 2)
        .max(Duration::from_millis(500))
        .as_nanos()
        .min(u128::from(u64::MAX));
    // Two separate statements: if a server rejects one spelling, the other
    // must still take effect (a combined statement fails atomically).
    let mut statements: Vec<PreDumpStatement> =
        ["master_heartbeat_period", "source_heartbeat_period"]
            .into_iter()
            .map(|var| PreDumpStatement {
                sql: format!("SET @{var} = {heartbeat_nanos}"),
                rejection_warning: None,
            })
            .collect();
    // `net_write_timeout` is a system variable, so it needs the `SESSION`
    // form — the `SET @net_write_timeout` spelling the heartbeats use would
    // define an unrelated user variable and leave the server default in place.
    // Its value has to be an integer literal: `MySQL` 8.x answers a function
    // expression there with `ER_WRONG_TYPE_FOR_VAR` (1232) and keeps the
    // inherited value, so the floor is resolved against the value read from
    // this connection rather than by a server-side `GREATEST`.
    //
    // Nothing is issued for a session already at or above the floor — there is
    // nothing to raise — and nothing is issued when the inherited value could
    // not be read, since assigning the floor blind is what would clamp an
    // operator's higher setting down. [`open_binlog_stream`] warns in that case.
    if inherited_net_write_timeout.is_some_and(|inherited| inherited < DUMP_NET_WRITE_TIMEOUT_SECS)
    {
        statements.push(PreDumpStatement {
            sql: format!("SET SESSION net_write_timeout = {DUMP_NET_WRITE_TIMEOUT_SECS}"),
            rejection_warning: Some(NET_WRITE_TIMEOUT_NOT_RAISED),
        });
    }
    statements
}

/// The `net_write_timeout`, in seconds, this dump connection has inherited.
///
/// Read on the connection because the floor cannot be resolved server-side: see
/// [`pre_dump_session_statements`] for why the assignment has to be a literal.
///
/// `Ok(None)` is a server that answered with SQL `NULL`, which is a different
/// thing from a read that failed and leads to the same floor being skipped for a
/// different reason — so the error is returned rather than flattened into the
/// `None`, and the caller says which one happened. Typed as `Option<u32>`
/// internally so a `NULL` answer decodes instead of panicking in `FromRow`.
async fn inherited_net_write_timeout(
    conn: &mut Conn,
) -> std::result::Result<Option<u32>, mysql_async::Error> {
    Ok(
        mysql_async::prelude::Queryable::query_first::<Option<u32>, _>(
            conn,
            "SELECT @@SESSION.net_write_timeout",
        )
        .await?
        .flatten(),
    )
}

/// `connection` is the shared source's `host:port` label, not a dataset: one dump
/// serves every `refresh_mode: changes` dataset on the source, so anything this
/// function reports is about the connection they share.
pub(super) async fn open_binlog_stream(
    params: &ReplicationParams,
    resume: &BinlogPosition,
    connection: &str,
    use_gtid: bool,
    gtid: &GtidSet,
) -> std::result::Result<BinlogStream, mysql_async::Error> {
    let mut conn = Conn::new(params.opts.clone()).await?;

    let inherited_timeout = match inherited_net_write_timeout(&mut conn).await {
        Ok(Some(inherited)) => Some(inherited),
        Ok(None) => {
            tracing::warn!(
                connection = %connection,
                "The MySQL source answered NULL for the binlog dump session's net_write_timeout on {connection}, so it was left as the source set it: {NET_WRITE_TIMEOUT_NOT_RAISED}"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                connection = %connection,
                error = %e,
                "Could not read the MySQL binlog dump session's net_write_timeout on {connection}, so it was left as the source set it: {NET_WRITE_TIMEOUT_NOT_RAISED}"
            );
            None
        }
    };

    for PreDumpStatement {
        sql,
        rejection_warning,
    } in pre_dump_session_statements(params.checkpoint_interval, inherited_timeout)
    {
        if let Err(e) = mysql_async::prelude::Queryable::query_drop(&mut conn, sql.as_str()).await {
            match rejection_warning {
                Some(consequence) => {
                    tracing::warn!(connection = %connection, statement = %sql, error = %e, "Failed to configure the MySQL binlog dump session on {connection}: {consequence}");
                }
                None => {
                    tracing::debug!(connection = %connection, statement = %sql, error = %e, "failed to set a binlog dump session variable");
                }
            }
        }
    }

    if use_gtid {
        // GTID auto-positioning: the server computes the start point from the
        // executed set (everything NOT in it is sent), so no filename/offset is
        // needed. This is what survives a failover — the set is
        // server-independent. An executed set that can't be represented on the
        // wire fails loudly rather than silently under-reporting.
        let gtid_set = gtid
            .to_sids()
            .map_err(|e| mysql_async::Error::Other(e.into()))?;
        conn.get_binlog_stream(
            BinlogStreamRequest::new(params.server_id)
                .with_gtid()
                .with_gtid_set(gtid_set),
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

/// Decode a rows event for the subscribed table into the transaction buffer.
pub fn buffer_rows_event(
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

/// Per-table row-image decoder prepared from a `TableMapEvent`.
///
/// It stores the column types, per-column metadata, and signedness needed to
/// parse row values without rebuilding that metadata for every row image.
/// Values still route through [`normalize_binlog_value`], so this path keeps
/// the same value normalization as [`buffer_rows_event`].
///
/// `pub` (not `pub(super)`) so `benches/mysql_binlog_replay.rs` can exercise
/// this exact decoder against the walk.
pub struct TableMapRowDecoder {
    /// Per source column: (binlog type, owned column metadata, unsigned).
    cols: Vec<(ColumnType, Vec<u8>, bool)>,
}

impl TableMapRowDecoder {
    /// Build from the route's `TableMapEvent`. Fails when the optional
    /// metadata does not parse or a column type is absent; callers can fall
    /// back to the [`buffer_rows_event`] walk for the same table map.
    pub fn try_new(tme: &TableMapEvent<'_>) -> Result<Self> {
        let decode_err = |message: String| Error::Decode { message };
        let extractor = OptionalMetaExtractor::new(tme.iter_optional_meta())
            .map_err(|e| decode_err(format!("table-map optional metadata: {e}")))?;
        let mut signedness = extractor.iter_signedness();
        let n = usize::try_from(tme.columns_count())
            .map_err(|_| decode_err("column count exceeds usize".to_string()))?;
        let mut cols = Vec::with_capacity(n);
        for i in 0..n {
            let ty = tme
                .get_column_type(i)
                .map_err(|e| decode_err(format!("column #{i} type: {e}")))?
                .ok_or_else(|| decode_err(format!("column #{i} type missing")))?;
            let meta = tme.get_column_metadata(i).unwrap_or(&[]).to_vec();
            let unsigned = ty
                .is_numeric_type()
                .then(|| signedness.next())
                .flatten()
                .unwrap_or_default();
            cols.push((ty, meta, unsigned));
        }
        Ok(Self { cols })
    }

    /// Decode one row image off `buf`. Spice requires `binlog_row_image = FULL`
    /// (enforced the same way the walk does: a missing column is a decode
    /// error), so `included` must cover every column.
    fn decode_image<'a>(
        &'a self,
        buf: &mut ParseBuf<'a>,
        included: &BitSlice<u8>,
        layout: &TableLayout,
    ) -> Result<Vec<Value>> {
        let num_included = included.count_ones();
        if num_included != self.cols.len() {
            return Err(Error::Decode {
                message: format!(
                    "row image carries {num_included} of {} columns. Spice requires \
                     `binlog_row_image = FULL` — a writer session overrode it.",
                    self.cols.len()
                ),
            });
        }
        let bitmap_bytes = num_included.div_ceil(8);
        let bitmap_buf: &[u8] = buf.parse(bitmap_bytes).map_err(|e| Error::Decode {
            message: format!("row null bitmap: {e}"),
        })?;
        let null_bitmap: &BitSlice<u8> = BitSlice::from_slice(bitmap_buf);
        let mut out = Vec::with_capacity(self.cols.len());
        for (idx, (ty, meta, unsigned)) in self.cols.iter().enumerate() {
            let column = layout.columns.get(idx).ok_or_else(|| Error::Decode {
                message: format!(
                    "row image has more columns than the validated layout ({})",
                    layout.columns.len()
                ),
            })?;
            let value: BinlogValue<'_> =
                if null_bitmap.get(idx).as_deref().copied().unwrap_or_default() {
                    BinlogValue::Value(Value::NULL)
                } else {
                    buf.parse((*ty, meta.as_slice(), *unsigned, false))
                        .map_err(|e| Error::Decode {
                            message: format!("column #{idx} (`{}`) value parse: {e}", column.name),
                        })?
                };
            out.push(normalize_binlog_value(column, value)?);
        }
        Ok(out)
    }
}

/// [`buffer_rows_event`] on the fast decode path: same op classification,
/// buffering, and metrics, with the per-row images parsed by the cached
/// [`TableMapRowDecoder`] instead of `mysql_common`'s per-image metadata rebuild.
pub fn buffer_rows_event_fast(
    rows_data: &RowsEventData<'_>,
    decoder: &TableMapRowDecoder,
    layout: &TableLayout,
    pk_source_indexes: &[usize],
    buffer: &mut TransactionBuffer,
    metrics: &MetricsCollector,
) -> Result<()> {
    enum FastOp {
        Insert,
        Update,
        Delete,
    }
    if layout.columns.len() != decoder.cols.len() {
        return Err(Error::Decode {
            message: format!(
                "row image has {} columns but the validated layout has {} — the source \
                 table was altered. Restart the dataset to re-validate the schema.",
                decoder.cols.len(),
                layout.columns.len()
            ),
        });
    }
    let op = match rows_data {
        RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => FastOp::Insert,
        RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_) => FastOp::Update,
        RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => FastOp::Delete,
        RowsEventData::PartialUpdateRowsEvent(_) => {
            return Err(Error::Decode {
                message: "partial-JSON row images are not supported. Set \
                          `binlog_row_value_options = ''` on the source server."
                    .to_string(),
            });
        }
    };
    let before_cols = rows_data.columns_before_image();
    let after_cols = rows_data.columns_after_image();
    let mut buf = ParseBuf(rows_data.rows_data());
    while !buf.0.is_empty() {
        let before = before_cols
            .map(|included| decoder.decode_image(&mut buf, included, layout))
            .transpose()?;
        let after = after_cols
            .map(|included| decoder.decode_image(&mut buf, included, layout))
            .transpose()?;
        match op {
            FastOp::Insert => {
                let after = after.ok_or_else(|| Error::Decode {
                    message: "write event is missing its after row image".to_string(),
                })?;
                buffer.push_insert(after);
                metrics.inc_insert();
            }
            FastOp::Update => {
                let before = before.ok_or_else(|| Error::Decode {
                    message: "update event is missing its before row image".to_string(),
                })?;
                let after = after.ok_or_else(|| Error::Decode {
                    message: "update event is missing its after row image".to_string(),
                })?;
                buffer.push_update(pk_source_indexes, before, after);
                metrics.inc_update();
            }
            FastOp::Delete => {
                let before = before.ok_or_else(|| Error::Decode {
                    message: "delete event is missing its before row image".to_string(),
                })?;
                buffer.push_delete(before);
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

/// Build an idle readiness heartbeat: a zero-row envelope stamped with a
/// source-attested clock (`source_now_ms`), flagged Ready when that clock is
/// within `ready_lag` of now. Emitted only when the stream has caught up to the
/// source head, so it never marks a still-behind dataset Ready.
pub(super) fn readiness_heartbeat(
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

pub(super) fn record_watermark(metrics: &MetricsCollector, event_timestamp: u32) {
    if event_timestamp > 0 {
        metrics.record_commit_watermark(
            SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(event_timestamp)),
        );
    }
}

pub(super) fn commit_ts_ms(event_timestamp: u32) -> Option<i64> {
    (event_timestamp > 0).then(|| i64::from(event_timestamp) * 1000)
}

/// Source row-image indexes of the declared primary keys, for PK-change
/// detection on UPDATE. `column_map` is dataset-field-indexed, so PK names
/// map through the dataset schema.
pub(super) fn compute_pk_source_indexes(
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
pub(super) struct AdoptedLayout {
    pub(super) layout: TableLayout,
    pub(super) column_map: Vec<usize>,
    pub(super) pk_source_indexes: Vec<usize>,
}

/// Re-fetch the source table's layout and reconcile it against the dataset
/// schema. Succeeds when every dataset column still exists on the source —
/// values keep decoding by name at their new positions. Source columns the
/// dataset doesn't declare (including ones a DDL just added) are not
/// replicated; newly-appeared ones are warned about, mirroring the Postgres
/// connector's block-mode behavior for compatible relation changes.
pub(super) async fn adopt_current_layout(
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

/// A coarse binlog column-type class, used to compare a layout fetched from
/// `information_schema` against the column types a `TableMap` event carries.
///
/// Deliberately coarse: families whose wire type depends on the server version
/// or the column's charset/metadata are collapsed into one class (or left
/// unmapped entirely) so that a class *disagreement* is always a real
/// disagreement. See [`source_type_class`] for what is intentionally omitted —
/// this comparison gates every decode, so a false positive would break a
/// healthy stream, which is strictly worse than the rare scramble it detects.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BinlogTypeClass {
    Int8,
    Int16,
    Int24,
    Int32,
    Int64,
    Float,
    Double,
    Decimal,
    Date,
    Year,
    Bit,
    Json,
    Geometry,
    /// `CHAR` / `VARCHAR` / `BINARY` / `VARBINARY` / `ENUM` / `SET`. One class
    /// because the wire type within this family depends on charset and on the
    /// `MYSQL_TYPE_STRING` metadata that encodes the real type.
    StringLike,
}

/// Classify an `information_schema.COLUMNS.COLUMN_TYPE` string, or `None` when
/// the binlog wire type is not pinned by the declared type alone.
///
/// Unmapped on purpose: `DATETIME` / `TIMESTAMP` / `TIME` (`*2` variants since
/// 5.6), `TEXT` / `BLOB` (all sent as `MYSQL_TYPE_BLOB`, distinguished only by
/// a length byte in the metadata), and `REAL` (`REAL_AS_FLOAT` `sql_mode`).
fn source_type_class(column_type: &str) -> Option<BinlogTypeClass> {
    let base: String = column_type
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_lowercase();
    match base.as_str() {
        "tinyint" => Some(BinlogTypeClass::Int8),
        "smallint" => Some(BinlogTypeClass::Int16),
        "mediumint" => Some(BinlogTypeClass::Int24),
        "int" | "integer" => Some(BinlogTypeClass::Int32),
        "bigint" => Some(BinlogTypeClass::Int64),
        "float" => Some(BinlogTypeClass::Float),
        "double" => Some(BinlogTypeClass::Double),
        "decimal" | "numeric" => Some(BinlogTypeClass::Decimal),
        "date" => Some(BinlogTypeClass::Date),
        "year" => Some(BinlogTypeClass::Year),
        "bit" => Some(BinlogTypeClass::Bit),
        "json" => Some(BinlogTypeClass::Json),
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geomcollection" | "geometrycollection" => {
            Some(BinlogTypeClass::Geometry)
        }
        "char" | "varchar" | "binary" | "varbinary" | "enum" | "set" => {
            Some(BinlogTypeClass::StringLike)
        }
        _ => None,
    }
}

/// Classify a `TableMap` column type, or `None` when it carries no usable
/// signal (see [`source_type_class`] for the omissions this mirrors).
fn event_type_class(column_type: ColumnType) -> Option<BinlogTypeClass> {
    match column_type {
        ColumnType::MYSQL_TYPE_TINY => Some(BinlogTypeClass::Int8),
        ColumnType::MYSQL_TYPE_SHORT => Some(BinlogTypeClass::Int16),
        ColumnType::MYSQL_TYPE_INT24 => Some(BinlogTypeClass::Int24),
        ColumnType::MYSQL_TYPE_LONG => Some(BinlogTypeClass::Int32),
        ColumnType::MYSQL_TYPE_LONGLONG => Some(BinlogTypeClass::Int64),
        ColumnType::MYSQL_TYPE_FLOAT => Some(BinlogTypeClass::Float),
        ColumnType::MYSQL_TYPE_DOUBLE => Some(BinlogTypeClass::Double),
        ColumnType::MYSQL_TYPE_NEWDECIMAL | ColumnType::MYSQL_TYPE_DECIMAL => {
            Some(BinlogTypeClass::Decimal)
        }
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => Some(BinlogTypeClass::Date),
        ColumnType::MYSQL_TYPE_YEAR => Some(BinlogTypeClass::Year),
        ColumnType::MYSQL_TYPE_BIT => Some(BinlogTypeClass::Bit),
        ColumnType::MYSQL_TYPE_JSON => Some(BinlogTypeClass::Json),
        ColumnType::MYSQL_TYPE_GEOMETRY => Some(BinlogTypeClass::Geometry),
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET => Some(BinlogTypeClass::StringLike),
        _ => None,
    }
}

/// A column position where the in-memory layout disagrees with the row images
/// the `TableMap` event describes.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct LayoutEventMismatch {
    pub(super) ordinal: usize,
    pub(super) column: String,
    pub(super) source_type: String,
}

/// Check the layout that will decode this table's row images against the
/// column types the `TableMap` event itself carries.
///
/// [`adopt_current_layout`] re-reads `information_schema`, which reports the
/// table's shape *now* — not its shape at the event being decoded. Under
/// replication lag the source can apply a second, same-column-count `ALTER`
/// (a reorder or a rename swap) before Spice reaches the first one, so the
/// adopted layout maps ordinals that the row images in flight do not use, and
/// values land in the wrong columns with nothing to fail on. The `TableMap`
/// event is the one description of the row image that travels *with* it, so it
/// is the authority here.
///
/// Returns the first position whose class disagrees, or `None` when the layout
/// is consistent with the event (including when nothing could be compared
/// confidently).
pub(super) fn layout_event_mismatch(
    layout: &TableLayout,
    tme: &TableMapEvent<'_>,
) -> Option<LayoutEventMismatch> {
    // `get_column_type` resolves the real type behind `MYSQL_TYPE_STRING`. An
    // out-of-range index, or a type this server build encodes in a way the
    // client doesn't recognize, yields nothing to compare — not a mismatch.
    layout_mismatch_against(layout, |ordinal| {
        tme.get_column_type(ordinal).ok().flatten()
    })
}

/// [`layout_event_mismatch`] over any source of per-ordinal column types.
fn layout_mismatch_against(
    layout: &TableLayout,
    event_type: impl Fn(usize) -> Option<ColumnType>,
) -> Option<LayoutEventMismatch> {
    layout
        .columns
        .iter()
        .enumerate()
        .find_map(|(ordinal, column)| {
            let source_class = source_type_class(&column.column_type)?;
            let event_class = event_type(ordinal).and_then(event_type_class)?;
            (source_class != event_class).then(|| LayoutEventMismatch {
                ordinal,
                column: column.name.clone(),
                source_type: column.column_type.clone(),
            })
        })
}

pub(super) fn purged_position_error(resume: &BinlogPosition, dataset_name: &str) -> StreamError {
    StreamError::External(format!(
        "mysql binlog for {dataset_name}: the source no longer has binlog position {resume} \
         (binary logs were purged). Restart the dataset with \
         `mysql_replication_invalid_checkpoint_behavior: restart` to drop the saved position \
         and re-snapshot the table, or increase `binlog_expire_logs_seconds` on the source."
    ))
}

/// See `postgres_replication::client` for the WARN→DEBUG demotion rationale:
/// the first failure of an outage is loud, the rest keep log volume sublinear.
pub(super) fn log_transient_reconnect(attempt: u32, dataset: &str, error: &str, retry_in_ms: u128) {
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

pub(super) enum QueryKind {
    Begin,
    Commit,
    /// `XA START|END|PREPARE|COMMIT|ROLLBACK ...` — two-phase transactions
    /// use a different commit protocol this stream does not implement.
    Xa,
    Statement,
}

pub(super) fn classify_query(statement: &str) -> QueryKind {
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
pub(super) enum StatementKind {
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
pub(super) fn classify_statement(
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
    use std::sync::Arc;

    use super::*;
    use crate::mysql_replication::setup::SourceColumn;

    /// `MySQL`'s documented default for `net_write_timeout`, which is what aborts
    /// a dump whose socket the pump stopped draining (#12527).
    const SERVER_DEFAULT_NET_WRITE_TIMEOUT_SECS: u32 = 60;

    fn statement_setting<'a>(
        statements: &'a [PreDumpStatement],
        variable: &str,
    ) -> Option<&'a str> {
        statements
            .iter()
            .find(|statement| statement.sql.contains(variable))
            .map(|statement| statement.sql.as_str())
    }

    /// The value a `SET` statement assigns: everything after its first `=`.
    ///
    /// `MySQL` answers an integer system variable assigned a non-integer value
    /// with `ER_WRONG_TYPE_FOR_VAR` (1232) and keeps the inherited setting, so
    /// what this returns has to parse as an integer.
    fn assigned_value(sql: &str) -> &str {
        sql.split_once('=')
            .map(|(_, value)| value.trim())
            .unwrap_or_default()
    }

    /// The warning the statement setting `variable` carries, if it carries one.
    fn statement_rejection_warning(
        statements: &[PreDumpStatement],
        variable: &str,
    ) -> Option<&'static str> {
        statements
            .iter()
            .find(|statement| statement.sql.contains(variable))
            .and_then(|statement| statement.rejection_warning)
    }

    #[test]
    fn the_dump_session_raises_a_lower_net_write_timeout_to_the_floor() {
        // Regression test for #12527: without this the source aborts the shared
        // dump 60s into one dataset's slow apply cycle, and every
        // `refresh_mode: changes` dataset on the connection re-streams from its
        // acked position while already behind.
        let statements = pre_dump_session_statements(
            Duration::from_secs(10),
            Some(SERVER_DEFAULT_NET_WRITE_TIMEOUT_SECS),
        );
        let sql = statement_setting(&statements, "net_write_timeout")
            .expect("a session below the floor must have net_write_timeout raised");
        assert_eq!(
            sql,
            format!("SET SESSION net_write_timeout = {DUMP_NET_WRITE_TIMEOUT_SECS}"),
            "net_write_timeout is a SYSTEM variable: the `SET @net_write_timeout` spelling the \
             heartbeats use would define an unrelated user variable and silently leave the \
             server default in place"
        );
        assert!(
            !sql.contains("SET @net_write_timeout"),
            "a user-variable spelling is inert for a system variable: {sql}"
        );
        const {
            assert!(
                DUMP_NET_WRITE_TIMEOUT_SECS > SERVER_DEFAULT_NET_WRITE_TIMEOUT_SECS,
                "raising the timeout to at most the server default would change nothing"
            );
        }
    }

    #[test]
    fn the_floor_is_assigned_as_an_integer_literal() {
        // Regression test for #13307. An integer system variable assigned a
        // function expression — `GREATEST(...)`, cast or not — is answered with
        // `ER_WRONG_TYPE_FOR_VAR` (1232): the assignment never takes effect, the
        // session keeps the server default, and the only trace is the rejection
        // warning. Every statement the dump session issues has to assign a value
        // the server will accept.
        let statements = pre_dump_session_statements(
            Duration::from_secs(10),
            Some(SERVER_DEFAULT_NET_WRITE_TIMEOUT_SECS),
        );
        let sql = statement_setting(&statements, "net_write_timeout")
            .expect("a session below the floor must have net_write_timeout raised");
        assert_eq!(
            assigned_value(sql).parse::<u32>().ok(),
            Some(DUMP_NET_WRITE_TIMEOUT_SECS),
            "the assigned value must be the floor as an integer literal, not an expression \
             the server will refuse: {sql}"
        );
    }

    #[test]
    fn the_dump_session_never_lowers_an_operators_higher_net_write_timeout() {
        // Raising `net_write_timeout` on the source is the manual workaround for
        // this symptom, so an operator who has already done it must not have it
        // lowered by connecting a runtime carrying this fix. The floor is
        // resolved against the value read from the connection, so a session at
        // or above it is left alone rather than assigned down to ours.
        for inherited in [
            DUMP_NET_WRITE_TIMEOUT_SECS,
            DUMP_NET_WRITE_TIMEOUT_SECS + 1,
            DUMP_NET_WRITE_TIMEOUT_SECS * 10,
        ] {
            let statements = pre_dump_session_statements(Duration::from_secs(10), Some(inherited));
            assert_eq!(
                statement_setting(&statements, "net_write_timeout"),
                None,
                "a session inheriting {inherited}s already clears the \
                 {DUMP_NET_WRITE_TIMEOUT_SECS}s floor, so nothing may be assigned to it"
            );
        }
    }

    #[test]
    fn an_unreadable_net_write_timeout_is_left_as_the_source_set_it() {
        // With no value read there is no way to tell a server default from an
        // operator's raised setting, and assigning the floor blind would clamp
        // the latter down. `open_binlog_stream` warns instead.
        let statements = pre_dump_session_statements(Duration::from_secs(10), None);
        assert_eq!(
            statement_setting(&statements, "net_write_timeout"),
            None,
            "an unread net_write_timeout must not be assigned a value"
        );
    }

    #[test]
    fn a_rejected_net_write_timeout_is_worth_a_warning() {
        // The heartbeat spellings are tried in pairs and one is always unknown,
        // so those must stay quiet; a rejected net_write_timeout leaves the
        // reconnect cliff in place and is the one the user can act on.
        let statements = pre_dump_session_statements(
            Duration::from_secs(10),
            Some(SERVER_DEFAULT_NET_WRITE_TIMEOUT_SECS),
        );
        for statement in &statements {
            assert_eq!(
                statement.rejection_warning.is_some(),
                statement.sql.contains("net_write_timeout"),
                "unexpected error visibility for {}",
                statement.sql
            );
        }
        let warning = statement_rejection_warning(&statements, "net_write_timeout")
            .expect("a rejected net_write_timeout must say what it costs");
        assert!(
            warning.contains("https://spiceai.org/docs/"),
            "the warning must point at the fix: {warning}"
        );
    }

    #[test]
    fn both_heartbeat_spellings_are_set_as_user_variables_in_nanoseconds() {
        // Half the checkpoint interval, in nanoseconds, on both the pre-8.4 and
        // 8.4 spellings — independent of whatever net_write_timeout the
        // session inherited.
        let statements = pre_dump_session_statements(
            Duration::from_secs(10),
            Some(SERVER_DEFAULT_NET_WRITE_TIMEOUT_SECS),
        );
        for var in ["master_heartbeat_period", "source_heartbeat_period"] {
            assert_eq!(
                statement_setting(&statements, var),
                Some(format!("SET @{var} = 5000000000").as_str()),
                "{var} must stay a user variable, in nanoseconds"
            );
        }
    }

    #[test]
    fn a_tiny_checkpoint_interval_floors_the_heartbeat_at_500ms() {
        // A sub-second interval would otherwise ask the source to heartbeat
        // continuously.
        let statements = pre_dump_session_statements(Duration::from_millis(100), None);
        assert_eq!(
            statement_setting(&statements, "master_heartbeat_period"),
            Some("SET @master_heartbeat_period = 500000000")
        );
    }

    /// A layout of `(name, COLUMN_TYPE)` columns in ordinal order.
    fn layout_of(columns: &[(&str, &str)]) -> TableLayout {
        TableLayout {
            columns: columns
                .iter()
                .map(|(name, column_type)| SourceColumn {
                    name: (*name).to_string(),
                    column_type: (*column_type).to_string(),
                    enum_variants: None,
                    set_variants: None,
                    is_primary_key: false,
                })
                .collect(),
        }
    }

    fn mismatch_against(
        columns: &[(&str, &str)],
        event: &[Option<ColumnType>],
    ) -> Option<LayoutEventMismatch> {
        layout_mismatch_against(&layout_of(columns), |ordinal| {
            event.get(ordinal).copied().flatten()
        })
    }

    #[test]
    fn layout_agreeing_with_the_event_is_not_a_mismatch() {
        assert_eq!(
            mismatch_against(
                &[("id", "int(11)"), ("name", "varchar(255)")],
                &[
                    Some(ColumnType::MYSQL_TYPE_LONG),
                    Some(ColumnType::MYSQL_TYPE_VARCHAR),
                ],
            ),
            None
        );
    }

    /// The #11764 scramble: two same-column-count ALTERs land while replication
    /// is behind, so the layout read for the first one is really the layout
    /// after the second — here `(id int, name varchar)` reordered to
    /// `(name varchar, id int)`. The row images in flight still use the old
    /// order, so this must be caught rather than decoded.
    #[test]
    fn a_reorder_read_ahead_of_the_event_is_a_mismatch() {
        let mismatch = mismatch_against(
            &[("name", "varchar(255)"), ("id", "int(11)")],
            &[
                Some(ColumnType::MYSQL_TYPE_LONG),
                Some(ColumnType::MYSQL_TYPE_VARCHAR),
            ],
        )
        .expect("a reordered layout must not decode against the old row image");
        assert_eq!(mismatch.ordinal, 0);
        assert_eq!(mismatch.column, "name");
        assert_eq!(mismatch.source_type, "varchar(255)");
    }

    /// The first disagreeing position is reported, not a later one.
    #[test]
    fn the_reported_mismatch_is_the_first_disagreeing_column() {
        let mismatch = mismatch_against(
            &[("a", "int"), ("b", "bigint"), ("c", "int")],
            &[
                Some(ColumnType::MYSQL_TYPE_LONG),
                Some(ColumnType::MYSQL_TYPE_LONG),
                Some(ColumnType::MYSQL_TYPE_LONG),
            ],
        )
        .expect("bigint at position 1 disagrees with a 4-byte int");
        assert_eq!(mismatch.ordinal, 1);
        assert_eq!(mismatch.column, "b");
    }

    /// Widths within the integer family are distinct classes — a swap between
    /// two integer columns of different widths still scrambles values.
    #[test]
    fn integer_widths_are_distinguished() {
        for (declared, event) in [
            ("tinyint(4)", ColumnType::MYSQL_TYPE_SHORT),
            ("smallint(6)", ColumnType::MYSQL_TYPE_INT24),
            ("mediumint(9)", ColumnType::MYSQL_TYPE_LONG),
            ("bigint(20)", ColumnType::MYSQL_TYPE_LONG),
        ] {
            assert!(
                mismatch_against(&[("n", declared)], &[Some(event)]).is_some(),
                "{declared} must not be accepted against {event:?}"
            );
        }
    }

    /// A column the event says nothing usable about is skipped, not flagged —
    /// the check only ever fails on a disagreement it is sure of.
    #[test]
    fn an_unreadable_event_type_is_skipped() {
        assert_eq!(
            mismatch_against(&[("id", "int(11)")], &[None]),
            None,
            "no event type to compare against is not a mismatch"
        );
        assert_eq!(
            mismatch_against(&[("id", "int(11)")], &[]),
            None,
            "an out-of-range ordinal is not a mismatch"
        );
    }

    /// Types whose wire encoding depends on the server version or the column's
    /// charset are deliberately unmapped, so they can never produce a false
    /// positive on a healthy stream. Guards the conservatism of the mapping.
    #[test]
    fn version_dependent_types_are_not_compared() {
        for declared in [
            "datetime(6)",
            "timestamp",
            "time(3)",
            "text",
            "longblob",
            "tinytext",
            "real",
        ] {
            assert_eq!(
                source_type_class(declared),
                None,
                "{declared} must stay unmapped: its wire type is not pinned by the declared type"
            );
            // ...and therefore never reports a mismatch, whatever the event says.
            assert_eq!(
                mismatch_against(&[("c", declared)], &[Some(ColumnType::MYSQL_TYPE_LONG)]),
                None
            );
        }
    }

    /// `CHAR`/`VARCHAR`/`BINARY`/`ENUM`/`SET` share one class: which wire type
    /// the server picks within the family depends on charset and metadata, so
    /// distinguishing them would fail healthy streams.
    #[test]
    fn the_string_family_is_one_class() {
        for declared in [
            "char(8)",
            "varchar(64)",
            "binary(16)",
            "varbinary(16)",
            "enum('a','b')",
            "set('a','b')",
        ] {
            for event in [
                ColumnType::MYSQL_TYPE_STRING,
                ColumnType::MYSQL_TYPE_VARCHAR,
                ColumnType::MYSQL_TYPE_VAR_STRING,
                ColumnType::MYSQL_TYPE_ENUM,
                ColumnType::MYSQL_TYPE_SET,
            ] {
                assert_eq!(
                    mismatch_against(&[("c", declared)], &[Some(event)]),
                    None,
                    "{declared} vs {event:?} must not be reported"
                );
            }
        }
        // But a string column against a numeric event still is a mismatch.
        assert!(
            mismatch_against(
                &[("c", "varchar(64)")],
                &[Some(ColumnType::MYSQL_TYPE_LONG)]
            )
            .is_some()
        );
    }

    #[test]
    fn source_types_are_parsed_from_their_base_name() {
        assert_eq!(
            source_type_class("int(10) unsigned zerofill"),
            Some(BinlogTypeClass::Int32)
        );
        assert_eq!(
            source_type_class("DECIMAL(10,2)"),
            Some(BinlogTypeClass::Decimal)
        );
        assert_eq!(
            source_type_class("numeric(5,0)"),
            Some(BinlogTypeClass::Decimal)
        );
        assert_eq!(
            source_type_class("bigint unsigned"),
            Some(BinlogTypeClass::Int64)
        );
        assert_eq!(source_type_class(""), None);
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
