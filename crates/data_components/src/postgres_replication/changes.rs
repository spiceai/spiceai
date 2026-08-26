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

//! Turn a stream of per-transaction `DecodedMessage`s into Arrow
//! [`crate::cdc::ChangeBatch`]es that the existing refresh loop knows how to
//! apply.

use std::borrow::Cow;
use std::num::NonZeroU32;
use std::sync::{Arc, atomic::AtomicU64};

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
        Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeStringBuilder,
        ListArray, RecordBatch, StringArray, StringBuilder, StringDictionaryBuilder, StructArray,
        Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
        UInt32Builder,
    },
    buffer::OffsetBuffer,
    datatypes::{
        DataType, Field, Int8Type, Int16Type, Int32Type, IntervalUnit, Schema, SchemaRef, TimeUnit,
    },
};
use async_trait::async_trait;
use snafu::ensure;

use super::pgoutput::{Relation, TupleData, Value};
use super::{PgOutputDecodeSnafu, Result, XidRegistry};
use crate::cdc::{
    ChangeBatch, ChangeBatchError, ChangeEnvelope, ChangeRows, CommitChange, CommitError,
    changes_schema,
};

/// Microseconds between the Unix epoch (1970-01-01) and the Postgres epoch
/// (2000-01-01). Binary `timestamp`/`timestamptz` are relative to the Postgres
/// epoch; Arrow timestamps are relative to the Unix epoch.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Days between the Unix epoch and the Postgres epoch. Binary `date` is days
/// since the Postgres epoch; Arrow `Date32` is days since the Unix epoch.
const PG_EPOCH_DAYS: i32 = 10_957;

/// One logical change derived from a pgoutput message.
#[derive(Debug, Clone)]
pub struct DecodedChange {
    pub op: ChangeOp,
    pub row: TupleData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Create,
    Update,
    Delete,
    Truncate,
}

impl ChangeOp {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "c",
            Self::Update => "u",
            Self::Delete => "d",
            Self::Truncate => "t",
        }
    }
}

/// Resolve `Value::Unchanged` (`TOAST`ed columns omitted from an UPDATE's new
/// tuple) by substituting the value from the old tuple, when `REPLICA
/// IDENTITY FULL` provides one — for an *unchanged* column, the old value IS
/// the current value, so the substitution is exact (the same merge Debezium
/// performs).
///
/// The old tuple may be key-only (pgoutput `K` tag under `REPLICA IDENTITY
/// DEFAULT` when key columns change): it has full column arity but NULLs for
/// non-key columns. A `u` marker implies the real value is non-NULL, so a
/// NULL old slot means "value not provided", never "value is NULL" — in that
/// case the marker is left in place and the change-batch build fails with the
/// actionable REPLICA-IDENTITY-FULL hint, instead of silently overwriting the
/// accelerator's value with NULL.
#[must_use]
pub fn merge_unchanged_toast(mut new: TupleData, old: Option<&TupleData>) -> TupleData {
    let Some(old) = old else {
        return new;
    };
    for (idx, column) in new.columns.iter_mut().enumerate() {
        if matches!(column, Some(Value::Unchanged))
            && let Some(Some(old_value)) = old.columns.get(idx)
            && !matches!(old_value, Value::Unchanged)
        {
            *column = Some(old_value.clone());
        }
    }
    new
}

/// Buffer a pgoutput UPDATE.
///
/// A Postgres primary-key update is represented as one UPDATE message with the
/// old key tuple plus the new row. Accelerators apply `ChangeOp::Update` as an
/// upsert keyed by the new primary key, so a primary-key change must also emit a
/// delete for the old key; otherwise the old accelerated row is orphaned.
pub fn push_update_change(
    changes: &mut Vec<DecodedChange>,
    relation: &Relation,
    old: Option<TupleData>,
    new: TupleData,
) {
    let new = merge_unchanged_toast(new, old.as_ref());
    let key_changed = old
        .as_ref()
        .is_some_and(|old| primary_key_changed(relation, old, &new));

    if key_changed && let Some(old) = old {
        changes.push(DecodedChange {
            op: ChangeOp::Delete,
            row: old,
        });
    }

    changes.push(DecodedChange {
        op: ChangeOp::Update,
        row: new,
    });
}

fn primary_key_changed(relation: &Relation, old: &TupleData, new: &TupleData) -> bool {
    relation
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.is_key)
        .any(|(idx, _)| {
            let old_value = old.columns.get(idx).and_then(Option::as_ref);
            let new_value = new.columns.get(idx).and_then(Option::as_ref);
            old_value.is_some() && new_value.is_some() && old_value != new_value
        })
}

/// Build a `ChangeBatch` from a list of decoded changes, typing the `data`
/// struct to the accelerator's Arrow schema.
///
/// **Nullability:** the `data` struct is built with every field marked
/// nullable *regardless* of the dataset schema's original nullability. This
/// is required because a DELETE event with REPLICA IDENTITY DEFAULT populates
/// only the primary-key columns in the old tuple; non-PK columns are sent as
/// null and would otherwise fail `StructArray::new` validation on non-null
/// schemas. Downstream consumers cast back to the accelerator's nullability
/// via `SchemaCastScanExec`, so the accelerator's stricter constraints still
/// apply on write.
pub fn build_change_batch(
    dataset_schema: &SchemaRef,
    relation: &Relation,
    changes: &[DecodedChange],
) -> Result<ChangeBatch> {
    let num_rows = changes.len();
    // Use a nullable-everywhere version of the schema for the ChangeBatch
    // wrapper — see the note above.
    let nullable_schema = nullable_clone(dataset_schema);
    let wrapper_schema = changes_schema(&nullable_schema);

    let mut op_builder = StringBuilder::with_capacity(num_rows, num_rows * 2);
    let primary_keys: Vec<&str> = relation
        .columns
        .iter()
        .filter(|c| c.is_key)
        .map(|c| c.name.as_str())
        .collect();
    let mut pk_offsets = Vec::<i32>::with_capacity(num_rows + 1);
    pk_offsets.push(0);
    let mut pk_values: Vec<&str> = Vec::with_capacity(num_rows.saturating_mul(primary_keys.len()));

    // One builder per output field, typed from dataset schema. Sized to the
    // transaction's row count: default-capacity builders reserve 1024 elements
    // per column, which turns a 1-row change on a wide table into a ~64 KB
    // allocation and inflates every byte-based CDC accounting downstream.
    let mut data_builders: Vec<FieldBuilder> = dataset_schema
        .fields()
        .iter()
        .map(|f| FieldBuilder::with_capacity(f.data_type(), num_rows))
        .collect::<Result<Vec<_>>>()?;

    // Precompute dataset_field_idx → relation_column_idx once per batch so the
    // hot path is O(rows × fields) rather than O(rows × fields²). A dataset
    // column absent from the relation maps to `None` and is applied as NULL:
    // schema validation (run on every Relation message, before any change is
    // buffered) has already established that only expected-absent columns —
    // Postgres GENERATED columns, which pgoutput never publishes — can be
    // missing here.
    let column_map: Vec<Option<usize>> = dataset_schema
        .fields()
        .iter()
        .map(|field| {
            relation
                .columns
                .iter()
                .position(|c| c.name == *field.name())
        })
        .collect();

    for change in changes {
        op_builder.append_value(change.op.as_str());
        pk_values.extend(primary_keys.iter().copied());
        pk_offsets.push(i32::try_from(pk_values.len()).map_err(|e| {
            super::Error::PgOutputDecode {
                message: format!("too many primary keys: {e}"),
            }
        })?);

        for (col_idx, source_idx) in column_map.iter().enumerate() {
            match source_idx {
                Some(source_idx) => {
                    let value = change.row.columns.get(*source_idx).and_then(Option::as_ref);
                    // The source column's Postgres type OID drives binary-format
                    // decoding; the text path ignores it.
                    let type_oid = relation.columns[*source_idx].type_oid;
                    data_builders[col_idx].append(value, change.op, type_oid)?;
                }
                None => data_builders[col_idx].append_null(),
            }
        }
    }

    let op_array: ArrayRef = Arc::new(op_builder.finish());
    let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
    let pk_list = ListArray::new(
        Arc::clone(&pk_field),
        OffsetBuffer::new(pk_offsets.into()),
        Arc::new(StringArray::from(pk_values)),
        None,
    );

    let data_columns: Vec<ArrayRef> = data_builders
        .into_iter()
        .map(FieldBuilder::finish)
        .collect();
    let data_struct = StructArray::new(nullable_schema.fields().clone(), data_columns, None);

    let record = RecordBatch::try_new(
        Arc::new(wrapper_schema),
        vec![op_array, Arc::new(pk_list), Arc::new(data_struct)],
    )
    .map_err(|e| super::Error::SchemaMismatch {
        message: format!("failed to build change record batch: {e}"),
    })?;

    ChangeBatch::try_new(record).map_err(|e| super::Error::SchemaMismatch {
        message: format!("change batch validation failed: {e}"),
    })
}

/// Decode a per-relation run of buffered raw pgoutput change messages (exactly
/// as the shared pump routed them, one `Bytes` per `XLogData` change message)
/// into `DecodedChange`s, applying the same per-op transform the pump used to
/// perform inline: INSERT → Create; UPDATE → (delete-of-old-key when the
/// primary key changed) + upsert, with unchanged-TOAST merged from the old
/// tuple; DELETE → Delete; TRUNCATE → Truncate. `relation` supplies the key
/// flags for the UPDATE primary-key-change split.
///
/// This runs on the per-dataset consumer (inside [`PgChangeRows::build`]), off
/// the shared pump — the pump only peeked each message's type + relation id to
/// route it. A throwaway [`super::pgoutput::Decoder`] is used because the
/// change decoders are structural (they don't consult the relation cache); the
/// `relation` argument, not the decoder, drives typing and key detection.
fn decode_raw_changes_iter<'a>(
    relation: &Relation,
    raw: impl Iterator<Item = &'a bytes::Bytes>,
    capacity: usize,
) -> Result<Vec<DecodedChange>> {
    use super::pgoutput::{DecodedMessage, Decoder};
    let mut decoder = Decoder::new();
    // Lower bound on capacity (a PK-changing UPDATE grows the vec by one).
    let mut changes = Vec::with_capacity(capacity);
    for msg in raw {
        match decoder.decode(msg.clone())? {
            DecodedMessage::Insert { tuple, .. } => changes.push(DecodedChange {
                op: ChangeOp::Create,
                row: tuple,
            }),
            DecodedMessage::Update { old, new, .. } => {
                push_update_change(&mut changes, relation, old, new);
            }
            DecodedMessage::Delete { old, .. } => changes.push(DecodedChange {
                op: ChangeOp::Delete,
                row: old,
            }),
            DecodedMessage::Truncate { .. } => changes.push(DecodedChange {
                op: ChangeOp::Truncate,
                row: TupleData { columns: vec![] },
            }),
            // Begin/Commit/Relation/Other are never buffered as per-relation
            // change messages; ignore defensively.
            DecodedMessage::Begin { .. }
            | DecodedMessage::Commit { .. }
            | DecodedMessage::Relation(_)
            | DecodedMessage::Other => {}
        }
    }
    Ok(changes)
}

/// Slice-taking wrapper over [`decode_raw_changes_iter`] for the tests that
/// assert the raw path against the eager one.
#[cfg(test)]
fn decode_raw_changes(relation: &Relation, raw: &[bytes::Bytes]) -> Result<Vec<DecodedChange>> {
    decode_raw_changes_iter(relation, raw.iter(), raw.len())
}

/// The raw change messages of one or more committed transactions for a single
/// relation, carried through the shared-slot [`ChangeEnvelope`] as a deferred
/// [`ChangeRows`] source.
///
/// The shared Postgres replication pump only peeks each message's type +
/// relation id to route it, then buffers the raw pgoutput bytes here;
/// [`ChangeRows::build`] decodes + transforms + Arrow-builds them later on the
/// per-dataset consumer (see [`decode_raw_changes_iter`] and
/// [`build_change_batch`]), moving the entire decode + O(rows × columns) build
/// off the single shared read path. Metadata is answered from the buffered bytes
/// without decoding.
///
/// Adjacent transactions for the same relation generation are folded together by
/// [`Self::try_append`], so one instance may span several source commits.
pub struct PgChangeRows {
    schema: SchemaRef,
    relation: Arc<Relation>,
    /// One raw pgoutput-message vector per source transaction. Keeping the
    /// transaction vectors as chunks makes pump-side envelope coalescing O(1):
    /// merging pushes a `Vec` instead of moving every `Bytes` while holding the
    /// member mailbox lock.
    raw_chunks: Vec<Vec<bytes::Bytes>>,
    /// The source transaction ids behind `raw_chunks`, tracked only for a member
    /// that can act on them. See [`ChunkXids`].
    chunk_xids: ChunkXids,
    source_commit_ts_ms: Option<i64>,
    /// Precomputed `num_rows_hint` (upper bound) and `encoded_len` so the
    /// consumer's coalescing/metric reads are O(1) rather than rescanning `raw`.
    row_hint: usize,
    byte_len: usize,
}

/// The source transaction ids behind [`PgChangeRows`]'s buffered chunks.
///
/// Tracking them serves exactly one purpose: letting a durable write-back
/// dataset recognize the echo of its own delivery. The shared pump therefore
/// tracks them per *member*, and only for a member that holds an echo-suppression
/// registry — every other dataset (which is every dataset in a deployment that
/// does not write through Spice) allocates nothing and does no per-commit
/// bookkeeping for a feature it cannot use, even while sharing a slot with a
/// write-back table.
enum ChunkXids {
    /// This member suppresses no echoes, so no xid was recorded. Holds no
    /// allocation.
    Untracked,
    /// One entry per `raw_chunks` entry, same length and index-aligned. `None`
    /// for a chunk built with no known xid (e.g. a test fixture); `xid` 0 is
    /// Postgres's "no transaction assigned" sentinel and is likewise stored as
    /// `None`, which is why the slot is a `NonZeroU32` — it also keeps the tag a
    /// compact 32 bits with no separate discriminant.
    Tracked(Vec<Option<NonZeroU32>>),
}

/// What one [`PgChangeRows::drop_echoed`] pass observed about the source
/// transactions buffered in an envelope.
#[derive(Default)]
pub(super) struct EchoScan {
    /// The xids dropped as this dataset's own write-back echo, for the caller to
    /// persist via [`XidRegistry::mark_commit_observed`].
    pub(super) dropped: Vec<u32>,
    /// Whether a chunk survived the filter carrying a transaction id this dataset
    /// did not issue — a write to this table from outside Spice. Drives the
    /// one-time external-writer report in
    /// [`super::shared::MemberMailboxReceiver::pop`].
    pub(super) saw_foreign_txn: bool,
}

impl PgChangeRows {
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        relation: Arc<Relation>,
        raw: Vec<bytes::Bytes>,
        source_commit_ts_ms: Option<i64>,
    ) -> Self {
        let (row_hint, byte_len) = Self::compute_hints(&schema, raw.iter());
        Self {
            schema,
            relation,
            raw_chunks: vec![raw],
            // Only a member that can act on the xids pays for them; see
            // `with_source_xid`, which the pump calls for exactly those members.
            chunk_xids: ChunkXids::Untracked,
            source_commit_ts_ms,
            row_hint,
            byte_len,
        }
    }

    /// Track the source `xid` (pgoutput's 32-bit stream xid) that produced this
    /// instance's one transaction chunk, so a later [`Self::drop_echoed`] can
    /// recognize whether it is the echo of this dataset's own write-back
    /// delivery. Called once, immediately after [`Self::new`] and before any
    /// [`Self::try_append`], and **only** for a member holding an
    /// echo-suppression registry — a member without one leaves the envelope
    /// [`ChunkXids::Untracked`] and allocates nothing.
    #[must_use]
    pub(super) fn with_source_xid(mut self, xid: Option<u32>) -> Self {
        // `xid` 0 is Postgres's "no transaction assigned" sentinel, so it
        // collapses to `None` alongside a genuinely absent xid.
        self.chunk_xids =
            ChunkXids::Tracked(vec![xid.and_then(NonZeroU32::new); self.raw_chunks.len()]);
        self
    }

    /// Compute `(row_hint, byte_len)` for a set of raw pgoutput messages
    /// against `schema` — see [`Self::new`]'s doc for what each term
    /// estimates. Shared with [`Self::drop_echoed`], which must recompute both
    /// after removing echoed chunks.
    fn compute_hints<'a>(
        schema: &SchemaRef,
        raw: impl Iterator<Item = &'a bytes::Bytes>,
    ) -> (usize, usize) {
        // Upper bound = one row per message, plus one more per UPDATE ('U')
        // since a primary-key-changing UPDATE expands to a delete + upsert.
        let mut count = 0usize;
        let mut updates = 0usize;
        let mut wire_bytes = 0usize;
        for message in raw {
            count += 1;
            if message.first() == Some(&b'U') {
                updates += 1;
            }
            wire_bytes += message.len();
        }
        let row_hint = count + updates;

        // Coalescing byte-budget estimate. Raw wire bytes alone under-count the
        // eventual Arrow memory for NULL / unchanged-TOAST / DELETE-key-only rows
        // (pgoutput sends those columns as 1-byte markers, but Arrow allocates the
        // full column width), so floor the estimate at the fixed-width Arrow
        // footprint derived from the schema. `max` tracks Arrow in both regimes
        // without a per-value scan: value-heavy rows → wire dominates;
        // NULL/delete-heavy → the fixed-width floor dominates.
        let per_row_fixed: usize = schema
            .fields()
            .iter()
            .map(|f| arrow_fixed_width(f.data_type()))
            .sum();
        let byte_len = wire_bytes.max(row_hint.saturating_mul(per_row_fixed));
        (row_hint, byte_len)
    }

    /// Append a compatible committed transaction without decoding or moving
    /// its individual pgoutput messages.
    ///
    /// Returns `other` unchanged unless both sides were built against the very
    /// same relation generation and working schema. A `Relation` message is the
    /// decoding contract for the raw tuple bytes, so combining messages across
    /// generations could interpret values with the wrong type or column layout.
    ///
    /// Compatibility is decided by pointer, not structure. Consecutive commits
    /// for one relation take their schema from the same cached route and their
    /// relation from the same decoder cache entry, so the pointers match on
    /// every mergeable pair; a new `Relation` (or an adopted schema widening)
    /// installs a fresh `Arc` and separates the generations. Pointer inequality
    /// on structurally identical inputs only declines a merge, never mis-decodes
    /// one — and this runs while the member mailbox lock is held, where a deep
    /// `Schema` (fields plus metadata) and per-column name comparison would be
    /// paid on every merge.
    pub(super) fn try_append(&mut self, mut other: Self) -> Option<Self> {
        if !Arc::ptr_eq(&self.schema, &other.schema)
            || !Arc::ptr_eq(&self.relation, &other.relation)
        {
            return Some(other);
        }

        // Both sides come from the same member, so both were built with the same
        // tracking decision. Declining a mixed merge costs a coalescing
        // opportunity at worst, and never mis-attributes an xid to a chunk.
        match (&mut self.chunk_xids, &mut other.chunk_xids) {
            (ChunkXids::Untracked, ChunkXids::Untracked) => {}
            (ChunkXids::Tracked(ours), ChunkXids::Tracked(theirs)) => ours.append(theirs),
            _ => return Some(other),
        }
        self.raw_chunks.append(&mut other.raw_chunks);
        self.row_hint = self.row_hint.saturating_add(other.row_hint);
        self.byte_len = self.byte_len.saturating_add(other.byte_len);
        self.source_commit_ts_ms = match (self.source_commit_ts_ms, other.source_commit_ts_ms) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left @ Some(_), None) => left,
            (None, right) => right,
        };
        None
    }

    /// Drop every buffered transaction chunk whose xid the registry recognizes
    /// as one of this dataset's own write-back deliveries — the echo of a
    /// commit Spice itself issued. Called by the per-dataset consumer
    /// ([`super::shared::MemberMailboxReceiver::pop`]), not the shared pump:
    /// [`XidRegistry::contains`] is a lock-free read of a small mirror, so
    /// filtering here costs nothing on the shared demux, and only the one
    /// dataset that actually produced the echo pays for decoding (and
    /// immediately discarding) it, rather than every table sharing the pump.
    ///
    /// Returns what the pass observed ([`EchoScan`]) — this method only reads the
    /// registry's lock-free membership mirror, never its own (async) state.
    pub(super) fn drop_echoed(&mut self, registry: &XidRegistry) -> EchoScan {
        // Compact both index-aligned vectors in place: keep a write cursor at the
        // next surviving slot, shift each kept chunk down over the gaps an echo
        // leaves, then truncate. When nothing echoes — the common case, since most
        // transactions carry an xid this dataset never wrote — the cursor stays in
        // lockstep with the scan, no swap runs, and the buffers (and their hints)
        // are left untouched, so a clean pop pays nothing beyond the membership
        // reads.
        // Unreachable in production: the pump tracks xids for exactly the members
        // that hold a registry, and only such a member calls this. Keeping every
        // chunk is the safe direction for a state that should not arise — it
        // delivers a change rather than discarding one.
        let ChunkXids::Tracked(xids) = &mut self.chunk_xids else {
            return EchoScan::default();
        };
        let mut scan = EchoScan::default();
        let mut kept = 0;
        for read in 0..xids.len() {
            if let Some(xid) = xids[read] {
                if registry.contains(xid.get()) {
                    // Leave the echoed chunk behind the write cursor; the final
                    // `truncate` discards it. Record its xid for the caller.
                    scan.dropped.push(xid.get());
                    continue;
                }
                // Surviving with a transaction id this dataset did not issue:
                // someone else wrote this table. A chunk with no xid is not
                // counted either way — only a positively identified foreign
                // transaction is.
                scan.saw_foreign_txn = true;
            }
            if read != kept {
                self.raw_chunks.swap(read, kept);
                xids.swap(read, kept);
            }
            kept += 1;
        }
        if scan.dropped.is_empty() {
            return scan;
        }
        self.raw_chunks.truncate(kept);
        xids.truncate(kept);
        let (row_hint, byte_len) =
            Self::compute_hints(&self.schema, self.raw_chunks.iter().flatten());
        self.row_hint = row_hint;
        self.byte_len = byte_len;
        scan
    }
}

/// Fixed per-value Arrow byte width for a data type, or 0 for variable-width
/// types (Utf8/Binary/List/Struct/…), whose bytes are already reflected in the
/// buffered pgoutput wire size. Used only to floor `PgChangeRows`'s coalescing
/// byte estimate at the real Arrow footprint (see `PgChangeRows::new`).
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

impl ChangeRows for PgChangeRows {
    fn is_empty(&self) -> bool {
        // Exact: every buffered change message yields at least one output row,
        // so no messages ⟺ no rows.
        self.raw_chunks.iter().all(Vec::is_empty)
    }

    fn num_rows_hint(&self) -> usize {
        // Upper bound (precomputed in `new`): one row per message + one per
        // UPDATE (a primary-key-changing UPDATE expands to delete + upsert).
        // Over-estimating only affects builder pre-allocation.
        self.row_hint
    }

    fn encoded_len(&self) -> usize {
        // Schema-aware coalescing-budget estimate (precomputed in `new`):
        // `max(wire_bytes, rows × fixed_width_footprint)`, a decode-free proxy
        // for the eventual Arrow memory that stays representative for both
        // value-heavy and NULL/delete-heavy bursts (see `new`). Still approximate
        // — Arrow allocation rounding and variable-column offsets aren't modeled —
        // so `max_coalesced_bytes` remains a soft bound backed by
        // `max_coalesced_envelopes`.
        self.byte_len
    }

    fn source_commit_ts_ms(&self) -> Option<i64> {
        self.source_commit_ts_ms
    }

    fn is_heartbeat(&self) -> bool {
        // WAL change batches always carry rows; readiness/keepalive heartbeats
        // are emitted separately as zero-row envelopes.
        false
    }

    fn build(self: Box<Self>) -> Result<ChangeBatch, ChangeBatchError> {
        let changes = decode_raw_changes_iter(
            &self.relation,
            self.raw_chunks.iter().flatten(),
            self.row_hint,
        )
        .map_err(|e| ChangeBatchError::DeferredBuild {
            message: e.to_string(),
        })?;
        build_change_batch(&self.schema, &self.relation, &changes)
            .map(|b| b.with_source_commit_ts_ms(self.source_commit_ts_ms))
            .map_err(|e| ChangeBatchError::DeferredBuild {
                message: e.to_string(),
            })
    }
}

/// Return a clone of `schema` where every field is marked nullable.
///
/// Used when building the internal `ChangeBatch` `data` struct — see the
/// comment on [`build_change_batch`] for why.
fn nullable_clone(schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Public alias used by `bootstrap::finish_batch` so the two code paths stay
/// in lockstep on `ChangeBatch` schema shape.
pub(super) fn nullable_clone_for_bootstrap(schema: &SchemaRef) -> SchemaRef {
    nullable_clone(schema)
}

/// Wrap a batch into a `ChangeEnvelope` whose `commit()` advances the
/// shared confirmed-flush LSN atomic.
#[must_use]
pub fn envelope_with_lsn(
    batch: ChangeBatch,
    confirmed_flush: Arc<AtomicU64>,
    flush_to: u64,
    is_dataset_ready: bool,
    dataset: String,
) -> ChangeEnvelope {
    // Capture the batch's source-commit timestamp before it's moved into the
    // envelope, so the committer can log end-to-end lag when it acks progress.
    let source_commit_ts_ms = batch.source_commit_ts_ms();
    ChangeEnvelope::new(
        Box::new(LsnCommitter {
            confirmed_flush,
            flush_to,
            dataset,
            source_commit_ts_ms,
        }),
        batch,
        is_dataset_ready,
    )
}

/// `CommitChange` impl that monotonically advances a shared LSN atomic.
/// The replication client's keepalive task periodically reads this atomic and
/// forwards it to Postgres as a `StandbyStatusUpdate`.
struct LsnCommitter {
    confirmed_flush: Arc<AtomicU64>,
    flush_to: u64,
    /// Dataset name, for the committer-progress log line.
    dataset: String,
    /// Source-commit timestamp (ms since the Unix epoch) of the batch this
    /// commit acks; `None` for snapshot-boundary batches.
    source_commit_ts_ms: Option<i64>,
}

#[async_trait]
impl CommitChange for LsnCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        use std::sync::atomic::Ordering;
        // Monotonic CAS loop: only advance; never regress.
        let mut current = self.confirmed_flush.load(Ordering::Relaxed);
        loop {
            if self.flush_to <= current {
                break;
            }
            match self.confirmed_flush.compare_exchange(
                current,
                self.flush_to,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        crate::cdc::log_committer_progress(
            "postgres",
            &self.dataset,
            &format!("lsn={}", self.flush_to),
            self.source_commit_ts_ms,
        );
        Ok(())
    }

    /// The Postgres logical replication slot retains WAL until `confirmed_flush`
    /// advances, and `commit()` here only advances that flush LSN. Deferring the
    /// commit therefore holds the slot back, so a crash before the deferred commit
    /// re-streams the un-acked tail from the slot — exactly-once via the idempotent
    /// apply. Safe to defer.
    fn supports_deferral(&self) -> bool {
        true
    }
}

/// Per-field Arrow builder that accepts `Option<&Value>` (text/null/unchanged)
/// and parses strings into the appropriate typed column.
///
/// Type coverage matches what `datafusion-table-providers`' Postgres provider
/// exposes via `read_provider()` — the dataset's Arrow schema flows through
/// that path, so mismatches here would fail `StructArray` validation.
///
/// `pub(super)` so the bootstrap path can reuse it for array columns (which it
/// fetches as `::text` literals — the same representation pgoutput delivers).
pub(super) enum FieldBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Binary(BinaryBuilder),
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt32(UInt32Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    Time64Nanos(Time64NanosecondBuilder),
    TimestampMicros(TimestampMicrosecondBuilder, Option<Arc<str>>),
    TimestampNanos(TimestampNanosecondBuilder, Option<Arc<str>>),
    /// `Decimal128(precision, scale)`
    Decimal128(Decimal128Builder, u8, i8),
    /// Dictionary-encoded string column — the Postgres provider maps ENUM
    /// columns to `Dictionary(Int8, Utf8)`. Values arrive as the enum's text
    /// label (pgoutput text format / `::text` on bootstrap).
    DictUtf8Int8(StringDictionaryBuilder<Int8Type>),
    DictUtf8Int16(StringDictionaryBuilder<Int16Type>),
    DictUtf8Int32(StringDictionaryBuilder<Int32Type>),
    /// Postgres array column (e.g. `text[]`, `int4[]`). Values arrive as
    /// Postgres array literals (`{a,"b c",NULL}`) — pgoutput's text format on
    /// the WAL path, and an explicit `::text` cast on the bootstrap path —
    /// and are parsed element-wise into the inner scalar builder.
    List {
        /// The dataset schema's exact element field — `ListArray` validation
        /// requires the produced child to match it precisely.
        item_field: Arc<Field>,
        inner: Box<FieldBuilder>,
        offsets: Vec<i32>,
        validity: Vec<bool>,
    },
}

impl FieldBuilder {
    /// Create a builder pre-sized for `capacity` values. Arrow's default
    /// builder constructors reserve 1024 elements per column, so an unsized
    /// builder makes small CDC batches (often a single row) allocate and
    /// report orders of magnitude more memory than the payload. String-like
    /// builders get a modest per-value byte estimate; under-estimates grow
    /// amortized, so a low guess is cheap.
    pub(super) fn with_capacity(data_type: &DataType, capacity: usize) -> Result<Self> {
        // Starting guess for variable-width data buffers (bytes per value).
        let data_capacity = capacity.saturating_mul(8);
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, data_capacity)),
            DataType::LargeUtf8 => {
                Self::LargeUtf8(LargeStringBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, data_capacity)),
            DataType::Boolean => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            DataType::Int8 => Self::Int8(Int8Builder::with_capacity(capacity)),
            DataType::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt32 => Self::UInt32(UInt32Builder::with_capacity(capacity)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::Time64Nanos(Time64NanosecondBuilder::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Self::TimestampMicros(
                TimestampMicrosecondBuilder::with_capacity(capacity),
                tz.clone(),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => Self::TimestampNanos(
                TimestampNanosecondBuilder::with_capacity(capacity),
                tz.clone(),
            ),
            DataType::Decimal128(precision, scale) => Self::Decimal128(
                Decimal128Builder::with_capacity(capacity).with_data_type(data_type.clone()),
                *precision,
                *scale,
            ),
            DataType::List(item_field) => {
                if matches!(
                    item_field.data_type(),
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
                ) {
                    return PgOutputDecodeSnafu {
                        message: format!(
                            "postgres_replication: multidimensional arrays are not supported \
                             ({data_type}). Cast the column to a scalar type on the source, \
                             or exclude the column from the dataset schema."
                        ),
                    }
                    .fail();
                }
                let mut offsets = Vec::with_capacity(capacity.saturating_add(1));
                offsets.push(0);
                Self::List {
                    item_field: Arc::clone(item_field),
                    // Element count per list is unknown; one element per row
                    // is a floor the inner builder grows past as needed.
                    inner: Box::new(Self::with_capacity(item_field.data_type(), capacity)?),
                    offsets,
                    validity: Vec::with_capacity(capacity),
                }
            }
            DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                return PgOutputDecodeSnafu {
                    message: format!(
                        "postgres_replication: list type {data_type} is not supported yet. \
                         Cast the column to a scalar type on the source, or exclude the \
                         column from the dataset schema."
                    ),
                }
                .fail();
            }
            // The Postgres provider maps ENUM columns to Dictionary(Int8, Utf8).
            // Keys are sized to the row count; the values side holds only the
            // distinct ENUM labels, which are few — start small and grow.
            DataType::Dictionary(key, value) if **value == DataType::Utf8 => match **key {
                DataType::Int8 => {
                    Self::DictUtf8Int8(StringDictionaryBuilder::with_capacity(capacity, 8, 128))
                }
                DataType::Int16 => {
                    Self::DictUtf8Int16(StringDictionaryBuilder::with_capacity(capacity, 8, 128))
                }
                DataType::Int32 => {
                    Self::DictUtf8Int32(StringDictionaryBuilder::with_capacity(capacity, 8, 128))
                }
                ref other => {
                    return PgOutputDecodeSnafu {
                        message: format!(
                            "postgres_replication: unsupported dictionary key type {other} \
                             (only Int8/Int16/Int32 keys with Utf8 values are supported)"
                        ),
                    }
                    .fail();
                }
            },
            DataType::Interval(_) => {
                return PgOutputDecodeSnafu {
                    message: "postgres_replication: INTERVAL columns are not supported yet. \
                              Cast to text or a numeric seconds value on the source."
                        .to_string(),
                }
                .fail();
            }
            other => {
                return PgOutputDecodeSnafu {
                    message: format!(
                        "postgres_replication: unsupported Arrow data type in dataset schema: {other}"
                    ),
                }
                .fail();
            }
        })
    }

    /// Append one pgoutput column value into the typed Arrow builder.
    ///
    /// `type_oid` is the source column's Postgres type OID (from the pgoutput
    /// `Relation` message). It is consulted only for binary-format values
    /// ([`Value::Binary`]); the text path is self-describing and ignores it.
    ///
    /// Under the binary output protocol Postgres tags each column `t` or `b`
    /// per-value, so *both* paths must remain live regardless of the requested
    /// format — a type without a binary send function still arrives as text.
    pub(super) fn append(
        &mut self,
        value: Option<&Value>,
        op: ChangeOp,
        type_oid: u32,
    ) -> Result<()> {
        let Some(v) = value else {
            self.append_null();
            return Ok(());
        };
        let s = match v {
            Value::Text(bytes) => {
                // UTF-8 validation is deferred to here (the decoder keeps raw
                // bytes) so it happens exactly once, on the way into the builder.
                std::str::from_utf8(bytes).map_err(|e| super::Error::PgOutputDecode {
                    message: format!("invalid utf8 in text value: {e}"),
                })?
            }
            Value::Unchanged => {
                // For UPDATE with a TOASTed column that wasn't changed, pgoutput
                // omits the value. Silently coercing to NULL would overwrite the
                // existing accelerator value — real data corruption. Fail loudly
                // so the operator sets REPLICA IDENTITY FULL or excludes the
                // column. For non-UPDATE ops this shouldn't appear.
                return PgOutputDecodeSnafu {
                    message: format!(
                        "postgres_replication: received Value::Unchanged (TOASTed column \
                         omitted) during {op:?} — this would silently overwrite the \
                         accelerator value with NULL. Set `ALTER TABLE ... REPLICA IDENTITY \
                         FULL;` on the source so the old tuple is sent with every update."
                    ),
                }
                .fail();
            }
            Value::Binary(bytes) => {
                // Binary output protocol: decode the type's `send` wire form
                // straight into the typed builder (no text round-trip).
                return self.append_binary(bytes, type_oid);
            }
        };
        match self {
            Self::Utf8(b) => b.append_value(s),
            Self::LargeUtf8(b) => b.append_value(s),
            Self::Binary(b) => {
                // Postgres text format for bytea uses `\x` hex escape (default
                // bytea_output=hex). Accept `\x<hex>`; legacy escape format
                // would be unusual on modern servers.
                let hex = s
                    .strip_prefix("\\x")
                    .ok_or_else(|| super::Error::PgOutputDecode {
                        message: format!(
                            "bytea text value did not start with \\x (got {chars} chars): \
                             configure `bytea_output = hex` on the source",
                            chars = s.chars().take(20).collect::<String>()
                        ),
                    })?;
                let bytes = decode_hex(hex).map_err(|e| super::Error::PgOutputDecode {
                    message: format!("bytea hex parse: {e}"),
                })?;
                b.append_value(bytes);
            }
            Self::Bool(b) => b.append_value(matches!(s, "t" | "true" | "TRUE")),
            Self::Int8(b) => {
                b.append_value(s.parse::<i8>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("int8 parse '{s}': {e}"),
                })?);
            }
            Self::Int16(b) => {
                b.append_value(s.parse::<i16>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("int16 parse '{s}': {e}"),
                })?);
            }
            Self::Int32(b) => {
                b.append_value(s.parse::<i32>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("int32 parse '{s}': {e}"),
                })?);
            }
            Self::Int64(b) => {
                b.append_value(s.parse::<i64>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("int64 parse '{s}': {e}"),
                })?);
            }
            Self::UInt32(b) => {
                b.append_value(s.parse::<u32>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("uint32 parse '{s}': {e}"),
                })?);
            }
            Self::Float32(b) => {
                b.append_value(s.parse::<f32>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("float32 parse '{s}': {e}"),
                })?);
            }
            Self::Float64(b) => {
                b.append_value(s.parse::<f64>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("float64 parse '{s}': {e}"),
                })?);
            }
            Self::Date32(b) => {
                let days = parse_pg_date_days_since_epoch(s)?;
                b.append_value(days);
            }
            Self::Time64Nanos(b) => {
                // Postgres text format for time: 'HH:MM:SS[.ffffff]'
                use chrono::Timelike;
                let t = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f").map_err(|e| {
                    super::Error::PgOutputDecode {
                        message: format!("time parse '{s}': {e}"),
                    }
                })?;
                let nanos_since_midnight = i64::from(t.num_seconds_from_midnight()) * 1_000_000_000
                    + i64::from(t.nanosecond());
                b.append_value(nanos_since_midnight);
            }
            Self::TimestampMicros(b, _tz) => {
                let micros = parse_pg_timestamp_micros(s)?;
                b.append_value(micros);
            }
            Self::TimestampNanos(b, _tz) => {
                let nanos = parse_pg_timestamp_nanos(s)?;
                b.append_value(nanos);
            }
            Self::Decimal128(b, precision, scale) => {
                let val = parse_pg_numeric_to_i128(s, *precision, *scale)?;
                b.append_value(val);
            }
            Self::DictUtf8Int8(b) => {
                b.append(s).map_err(|e| super::Error::PgOutputDecode {
                    message: format!("dictionary append '{s}': {e}"),
                })?;
            }
            Self::DictUtf8Int16(b) => {
                b.append(s).map_err(|e| super::Error::PgOutputDecode {
                    message: format!("dictionary append '{s}': {e}"),
                })?;
            }
            Self::DictUtf8Int32(b) => {
                b.append(s).map_err(|e| super::Error::PgOutputDecode {
                    message: format!("dictionary append '{s}': {e}"),
                })?;
            }
            Self::List {
                item_field,
                inner,
                offsets,
                validity,
            } => {
                let elements = parse_pg_array_literal(s)?;
                for element in &elements {
                    match element {
                        Some(text) => {
                            // Text array literal: each element is itself text,
                            // so the inner builder's text path handles it and
                            // the element `type_oid` is irrelevant (`0`).
                            let value = Value::Text(bytes::Bytes::from(text.clone()));
                            inner.append(Some(&value), op, 0)?;
                        }
                        None if item_field.is_nullable() => inner.append(None, op, 0)?,
                        None => {
                            return PgOutputDecodeSnafu {
                                message: format!(
                                    "NULL array element for non-nullable list item field \
                                     `{}`",
                                    item_field.name()
                                ),
                            }
                            .fail();
                        }
                    }
                }
                let end = offsets.last().copied().unwrap_or(0)
                    + i32::try_from(elements.len()).map_err(|e| super::Error::PgOutputDecode {
                        message: format!("array too large: {e}"),
                    })?;
                offsets.push(end);
                validity.push(true);
            }
        }
        Ok(())
    }

    /// Decode a binary (`send`-format) pgoutput value directly into the typed
    /// builder. Reached when Postgres tags the column `b` under the binary
    /// output protocol. `type_oid` names the source type for diagnostics.
    fn append_binary(&mut self, raw: &[u8], type_oid: u32) -> Result<()> {
        use postgres_protocol::types as pg;

        // Map a postgres-protocol binary-decode error into our decode error,
        // naming the failing logical type and OID for actionable diagnostics.
        // `kind` is always a string literal, so `'static` lets the returned
        // closure capture it without a borrow that would outlive the call.
        let decode_err = |kind: &'static str| {
            move |e: Box<dyn std::error::Error + Sync + Send>| super::Error::PgOutputDecode {
                message: format!(
                    "postgres_replication: binary {kind} decode failed (type oid {type_oid}): {e}"
                ),
            }
        };
        let range_err = |kind: &str, detail: String| super::Error::PgOutputDecode {
            message: format!(
                "postgres_replication: binary {kind} value out of range (type oid {type_oid}): \
                 {detail}"
            ),
        };

        match self {
            Self::Bool(b) => b.append_value(pg::bool_from_sql(raw).map_err(decode_err("bool"))?),
            Self::Int8(b) => {
                b.append_value(pg::char_from_sql(raw).map_err(decode_err("\"char\""))?);
            }
            Self::Int16(b) => b.append_value(pg::int2_from_sql(raw).map_err(decode_err("int2"))?),
            Self::Int32(b) => b.append_value(pg::int4_from_sql(raw).map_err(decode_err("int4"))?),
            Self::Int64(b) => b.append_value(pg::int8_from_sql(raw).map_err(decode_err("int8"))?),
            Self::UInt32(b) => b.append_value(pg::oid_from_sql(raw).map_err(decode_err("oid"))?),
            Self::Float32(b) => {
                b.append_value(pg::float4_from_sql(raw).map_err(decode_err("float4"))?);
            }
            Self::Float64(b) => {
                b.append_value(pg::float8_from_sql(raw).map_err(decode_err("float8"))?);
            }
            // A column mapped to Arrow Utf8 can be a genuine text type (whose
            // binary send form IS UTF-8 text) or a non-text type Postgres still
            // maps to a string (uuid/inet/cidr/macaddr). `decode_binary_text`
            // dispatches on the OID and yields the canonical Postgres text —
            // identical to what the `::text` bootstrap path produces, so the
            // snapshot and WAL agree.
            Self::Utf8(b) => b.append_value(decode_binary_text(raw, type_oid)?.as_ref()),
            Self::LargeUtf8(b) => b.append_value(decode_binary_text(raw, type_oid)?.as_ref()),
            // bytea `send` form is the raw payload verbatim; append it directly
            // (the one copy into the Arrow buffer is unavoidable).
            Self::Binary(b) => b.append_value(raw),
            Self::Date32(b) => {
                let pg_days = pg::date_from_sql(raw).map_err(decode_err("date"))?;
                let days = pg_days
                    .checked_add(PG_EPOCH_DAYS)
                    .ok_or_else(|| range_err("date", format!("pg days {pg_days}")))?;
                b.append_value(days);
            }
            Self::Time64Nanos(b) => {
                let micros = pg::time_from_sql(raw).map_err(decode_err("time"))?;
                let nanos = micros
                    .checked_mul(1_000)
                    .ok_or_else(|| range_err("time", format!("micros {micros}")))?;
                b.append_value(nanos);
            }
            Self::TimestampMicros(b, _tz) => {
                let pg_micros = pg::timestamp_from_sql(raw).map_err(decode_err("timestamp"))?;
                let micros = pg_micros
                    .checked_add(PG_EPOCH_MICROS)
                    .ok_or_else(|| range_err("timestamp", format!("pg micros {pg_micros}")))?;
                b.append_value(micros);
            }
            Self::TimestampNanos(b, _tz) => {
                let pg_micros = pg::timestamp_from_sql(raw).map_err(decode_err("timestamp"))?;
                let micros = pg_micros
                    .checked_add(PG_EPOCH_MICROS)
                    .ok_or_else(|| range_err("timestamp", format!("pg micros {pg_micros}")))?;
                let nanos = micros
                    .checked_mul(1_000)
                    .ok_or_else(|| range_err("timestamp", format!("micros {micros}")))?;
                b.append_value(nanos);
            }
            Self::Decimal128(b, precision, scale) => {
                let v = numeric_from_binary(raw, *precision, *scale)?;
                b.append_value(v);
            }
            Self::DictUtf8Int8(b) => {
                b.append(pg::text_from_sql(raw).map_err(decode_err("enum"))?)
                    .map_err(|e| super::Error::PgOutputDecode {
                        message: format!("dictionary append: {e}"),
                    })?;
            }
            Self::DictUtf8Int16(b) => {
                b.append(pg::text_from_sql(raw).map_err(decode_err("enum"))?)
                    .map_err(|e| super::Error::PgOutputDecode {
                        message: format!("dictionary append: {e}"),
                    })?;
            }
            Self::DictUtf8Int32(b) => {
                b.append(pg::text_from_sql(raw).map_err(decode_err("enum"))?)
                    .map_err(|e| super::Error::PgOutputDecode {
                        message: format!("dictionary append: {e}"),
                    })?;
            }
            Self::List {
                item_field,
                inner,
                offsets,
                validity,
            } => {
                if matches!(
                    item_field.data_type(),
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
                ) {
                    return PgOutputDecodeSnafu {
                        message:
                            "postgres_replication: multidimensional arrays are not supported. \
                                  Cast the column to a scalar type on the source, or exclude the \
                                  column from the dataset schema."
                                .to_string(),
                    }
                    .fail();
                }
                let (elem_oid, elements) = decode_binary_array(raw)?;
                let count = elements.len();
                for element in elements {
                    match element {
                        // Recurse into the inner builder's binary path with the
                        // element slice directly — no per-element allocation.
                        Some(elem) => inner.append_binary(elem, elem_oid)?,
                        None if item_field.is_nullable() => inner.append_null(),
                        None => {
                            return PgOutputDecodeSnafu {
                                message: format!(
                                    "NULL array element for non-nullable list item field `{}`",
                                    item_field.name()
                                ),
                            }
                            .fail();
                        }
                    }
                }
                let end = offsets.last().copied().unwrap_or(0)
                    + i32::try_from(count).map_err(|e| super::Error::PgOutputDecode {
                        message: format!("array too large: {e}"),
                    })?;
                offsets.push(end);
                validity.push(true);
            }
        }
        Ok(())
    }

    pub(super) fn append_null(&mut self) {
        match self {
            Self::Utf8(b) => b.append_null(),
            Self::LargeUtf8(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::Bool(b) => b.append_null(),
            Self::Int8(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt32(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::Time64Nanos(b) => b.append_null(),
            Self::TimestampMicros(b, _) => b.append_null(),
            Self::TimestampNanos(b, _) => b.append_null(),
            Self::Decimal128(b, _, _) => b.append_null(),
            Self::DictUtf8Int8(b) => b.append_null(),
            Self::DictUtf8Int16(b) => b.append_null(),
            Self::DictUtf8Int32(b) => b.append_null(),
            Self::List {
                offsets, validity, ..
            } => {
                // NULL array: empty slot, validity false.
                offsets.push(offsets.last().copied().unwrap_or(0));
                validity.push(false);
            }
        }
    }

    pub(super) fn finish(mut self) -> ArrayRef {
        match &mut self {
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::LargeUtf8(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Int8(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt32(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::Time64Nanos(b) => Arc::new(b.finish()),
            Self::TimestampMicros(b, tz) => {
                let arr = b.finish();
                Arc::new(match tz {
                    Some(tz) => arr.with_timezone(Arc::clone(tz)),
                    None => arr,
                })
            }
            Self::TimestampNanos(b, tz) => {
                let arr = b.finish();
                Arc::new(match tz {
                    Some(tz) => arr.with_timezone(Arc::clone(tz)),
                    None => arr,
                })
            }
            Self::Decimal128(b, _, _) => Arc::new(b.finish()),
            Self::DictUtf8Int8(b) => Arc::new(b.finish()),
            Self::DictUtf8Int16(b) => Arc::new(b.finish()),
            Self::DictUtf8Int32(b) => Arc::new(b.finish()),
            Self::List {
                item_field,
                inner,
                offsets,
                validity,
            } => {
                let values =
                    std::mem::replace(inner, Box::new(FieldBuilder::Utf8(StringBuilder::new())))
                        .finish();
                let nulls = if validity.iter().all(|v| *v) {
                    None
                } else {
                    Some(arrow::buffer::NullBuffer::from(std::mem::take(validity)))
                };
                Arc::new(ListArray::new(
                    Arc::clone(item_field),
                    OffsetBuffer::new(std::mem::take(offsets).into()),
                    values,
                    nulls,
                ))
            }
        }
    }
}

/// Parse a Postgres array literal (the text representation pgoutput emits and
/// `::text` produces), e.g. `{a,"b c",NULL,"quo\"te"}`, into its elements.
/// `None` represents a NULL element.
///
/// Handles: empty arrays (`{}`), double-quoted elements with `\"` / `\\`
/// escapes, unquoted `NULL` (case-insensitive), and a dimension-bounds prefix
/// (`[0:1]={...}`). Multidimensional arrays (nested `{`) are rejected — the
/// Arrow side only supports single-level lists.
fn parse_pg_array_literal(s: &str) -> Result<Vec<Option<String>>> {
    let err = |reason: &str| super::Error::PgOutputDecode {
        message: format!("postgres array literal parse '{s}': {reason}"),
    };

    // Arrays with non-default lower bounds are prefixed: `[0:1]={a,b}`.
    let trimmed = s.trim();
    let body = if let Some(rest) = trimmed.strip_prefix('[') {
        let eq = rest
            .find("]=")
            .ok_or_else(|| err("unterminated bounds prefix"))?;
        &rest[eq + 2..]
    } else {
        trimmed
    };
    let body = body
        .strip_prefix('{')
        .and_then(|b| b.strip_suffix('}'))
        .ok_or_else(|| err("expected {...}"))?;
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let mut elements = Vec::new();
    let mut current = String::new();
    let mut chars = body.chars();
    let mut in_quotes = false;
    let mut was_quoted = false;

    let finish_element =
        |current: &mut String, was_quoted: bool, elements: &mut Vec<Option<String>>| {
            let text = std::mem::take(current);
            if !was_quoted && text.eq_ignore_ascii_case("NULL") {
                elements.push(None);
            } else {
                elements.push(Some(text));
            }
        };

    while let Some(ch) = chars.next() {
        if in_quotes {
            match ch {
                '\\' => {
                    let escaped = chars.next().ok_or_else(|| err("dangling escape"))?;
                    current.push(escaped);
                }
                '"' => in_quotes = false,
                other => current.push(other),
            }
            continue;
        }
        match ch {
            '"' => {
                in_quotes = true;
                was_quoted = true;
            }
            ',' => {
                finish_element(&mut current, was_quoted, &mut elements);
                was_quoted = false;
            }
            '{' => return Err(err("multidimensional arrays are not supported")),
            other => current.push(other),
        }
    }
    if in_quotes {
        return Err(err("unterminated quoted element"));
    }
    finish_element(&mut current, was_quoted, &mut elements);
    Ok(elements)
}

fn parse_pg_date_days_since_epoch(s: &str) -> Result<i32> {
    let parsed = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| {
        super::Error::PgOutputDecode {
            message: format!("date parse '{s}': {e}"),
        }
    })?;
    let Some(epoch) = chrono::NaiveDate::from_ymd_opt(1970, 1, 1) else {
        unreachable!("1970-01-01 is a valid NaiveDate")
    };
    let days = (parsed - epoch).num_days();
    i32::try_from(days).map_err(|e| super::Error::PgOutputDecode {
        message: format!("date overflow: {e}"),
    })
}

fn parse_pg_timestamp_nanos(s: &str) -> Result<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return dt
            .timestamp_nanos_opt()
            .ok_or_else(|| super::Error::PgOutputDecode {
                message: format!("timestamp '{s}' out of nanosecond range"),
            });
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return dt
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or_else(|| super::Error::PgOutputDecode {
                message: format!("timestamp '{s}' out of nanosecond range"),
            });
    }
    PgOutputDecodeSnafu {
        message: format!("timestamp parse '{s}' failed"),
    }
    .fail()
}

/// Parse a Postgres NUMERIC text value (e.g. "123.456", "-7", "1e10",
/// "NaN", "Infinity") into an `i128` with the given Arrow scale.
///
/// We reject `NaN` / `Infinity` since Decimal128 has no representation for
/// them. Scientific notation is expanded. Values whose scale is less than the
/// Arrow scale are zero-padded on the right.
/// Public wrapper: parse a Postgres NUMERIC text value to `i128` with the
/// dataset's scale. Bootstrap reuses this so we only have one numeric parsing
/// implementation.
pub(super) fn parse_pg_numeric_public(s: &str, scale: i8) -> Result<i128> {
    parse_pg_numeric_to_i128(s, 38, scale)
}

/// Decode a Postgres binary `numeric` (`send` wire form) into an `i128` scaled
/// to the dataset's declared Arrow scale.
///
/// Wire format: `i16 ndigits, i16 weight, u16 sign, u16 dscale`, then `ndigits`
/// base-10000 groups (`i16` each, most-significant first). The value is
/// `sign · Σ digit[i]·10000^(weight−i)`. We fold the groups into a base-10000
/// integer `m` and rescale by `10^(4·(weight−(ndigits−1)) + scale)`; a negative
/// exponent that does not divide `m` evenly means the value carries more
/// fractional precision than the declared scale — an error, never a silent
/// round (mirrors the text path's scale check). `NaN`/`±Infinity` sign words
/// are rejected: `Decimal128` cannot represent them.
fn numeric_from_binary(raw: &[u8], precision: u8, scale: i8) -> Result<i128> {
    use bytes::Buf;

    const NUMERIC_POS: u16 = 0x0000;
    const NUMERIC_NEG: u16 = 0x4000;

    let mut b = raw;
    ensure!(
        b.remaining() >= 8,
        PgOutputDecodeSnafu {
            message: "short binary numeric header".to_string()
        }
    );
    let ndigits = b.get_u16();
    let weight = b.get_i16();
    let sign = b.get_u16();
    let _dscale = b.get_u16();

    let negative = match sign {
        NUMERIC_POS => false,
        NUMERIC_NEG => true,
        other => {
            return PgOutputDecodeSnafu {
                message: format!(
                    "postgres_replication: numeric special value (sign 0x{other:04x}, \
                     NaN/Infinity) is not representable as Decimal128"
                ),
            }
            .fail();
        }
    };

    ensure!(
        b.remaining() >= usize::from(ndigits) * 2,
        PgOutputDecodeSnafu {
            message: "short binary numeric digits".to_string()
        }
    );

    let overflow = || super::Error::PgOutputDecode {
        message: "postgres_replication: numeric magnitude exceeds Decimal128 range".to_string(),
    };

    let mut m: i128 = 0;
    for _ in 0..ndigits {
        let d = b.get_u16();
        ensure!(
            d < 10_000,
            PgOutputDecodeSnafu {
                message: format!("postgres_replication: invalid base-10000 numeric digit {d}")
            }
        );
        m = m
            .checked_mul(10_000)
            .and_then(|m| m.checked_add(i128::from(d)))
            .ok_or_else(overflow)?;
    }

    // result = m · 10^p, where p rescales the least-significant base-10000 group
    // (exponent weight−(ndigits−1), i.e. ×10^(4·that)) to the declared scale.
    let e_min = i64::from(weight) - (i64::from(ndigits) - 1);
    let p = 4 * e_min + i64::from(scale);

    let result = if p >= 0 {
        let exp = u32::try_from(p).map_err(|_| overflow())?;
        m.checked_mul(pow10_i128(exp)?).ok_or_else(overflow)?
    } else {
        let exp = u32::try_from(-p).map_err(|_| overflow())?;
        let pow = pow10_i128(exp)?;
        ensure!(
            m % pow == 0,
            PgOutputDecodeSnafu {
                message: format!(
                    "postgres_replication: numeric value carries more fractional precision \
                     than the dataset's declared scale {scale}"
                )
            }
        );
        m / pow
    };

    let signed = if negative { -result } else { result };
    ensure_decimal_precision(signed, precision)?;
    Ok(signed)
}

/// Ensure a decoded unscaled `Decimal128` value fits the column's declared
/// precision (`abs(value) < 10^precision`).
///
/// Postgres enforces precision on the source column, so a violation means the
/// dataset schema declares a narrower precision than the source — surface it as
/// a structured error rather than storing a value Arrow would treat as out of
/// range for the declared type. Shared by the text and binary numeric decoders
/// so both agree on what's representable.
fn ensure_decimal_precision(value: i128, precision: u8) -> Result<()> {
    // Saturates for precision >= 39; that's fine — `i128`'s magnitude never
    // reaches `u128::MAX`, and Arrow caps `Decimal128` precision at 38 anyway.
    let mut bound: u128 = 1;
    for _ in 0..precision {
        bound = bound.saturating_mul(10);
    }
    ensure!(
        value.unsigned_abs() < bound,
        PgOutputDecodeSnafu {
            message: format!(
                "postgres_replication: numeric value exceeds the dataset's declared \
                 Decimal128 precision {precision}"
            )
        }
    );
    Ok(())
}

/// `10^exp` as `i128`, erroring if it overflows `Decimal128`'s range.
fn pow10_i128(exp: u32) -> Result<i128> {
    let mut v: i128 = 1;
    for _ in 0..exp {
        v = v
            .checked_mul(10)
            .ok_or_else(|| super::Error::PgOutputDecode {
                message: format!(
                    "postgres_replication: numeric magnitude 10^{exp} exceeds Decimal128 range"
                ),
            })?;
    }
    Ok(v)
}

/// Parse a Postgres binary array (`send` wire form) into its element OID and a
/// row-major list of element payloads (`None` = SQL NULL). Only 0- and
/// 1-dimensional arrays are supported — matching the text path, which rejects
/// multidimensional arrays. Element slices borrow from `raw`.
fn decode_binary_array(raw: &[u8]) -> Result<(u32, Vec<Option<&[u8]>>)> {
    use bytes::Buf;

    let mut b = raw;
    ensure!(
        b.remaining() >= 12,
        PgOutputDecodeSnafu {
            message: "short binary array header".to_string()
        }
    );
    let ndim = b.get_i32();
    let _flags = b.get_i32();
    let elem_oid = b.get_u32();
    ensure!(
        (0..=1).contains(&ndim),
        PgOutputDecodeSnafu {
            message: format!(
                "postgres_replication: unsupported array dimensionality {ndim} \
                 (only empty or 1-dimensional arrays of scalars are supported). \
                 Cast the column to a scalar type."
            )
        }
    );

    let mut count: usize = 0;
    if ndim == 1 {
        ensure!(
            b.remaining() >= 8,
            PgOutputDecodeSnafu {
                message: "short binary array dimension".to_string()
            }
        );
        let len = b.get_i32();
        let _lower_bound = b.get_i32();
        count = usize::try_from(len).map_err(|_| super::Error::PgOutputDecode {
            message: format!("postgres_replication: negative array dimension {len}"),
        })?;
    }

    // Fallibly reserve so a corrupt/oversized `count` from the WAL surfaces as a
    // structured error instead of aborting the process on a huge allocation.
    let mut out = Vec::new();
    out.try_reserve_exact(count)
        .map_err(|e| super::Error::PgOutputDecode {
            message: format!("postgres_replication: array too large (len {count}): {e}"),
        })?;
    for _ in 0..count {
        ensure!(
            b.remaining() >= 4,
            PgOutputDecodeSnafu {
                message: "short binary array element length".to_string()
            }
        );
        let raw_len = b.get_i32();
        if raw_len < 0 {
            out.push(None);
        } else {
            let elem_len = usize::try_from(raw_len).map_err(|e| super::Error::PgOutputDecode {
                message: format!("invalid array element length: {e}"),
            })?;
            ensure!(
                b.remaining() >= elem_len,
                PgOutputDecodeSnafu {
                    message: "short binary array element body".to_string()
                }
            );
            let (elem, rest) = b.split_at(elem_len);
            b = rest;
            out.push(Some(elem));
        }
    }
    Ok((elem_oid, out))
}

/// Decode a binary value destined for an Arrow `Utf8`/`LargeUtf8` column into
/// its canonical Postgres text, dispatched by the source type OID.
///
/// Most Arrow-`Utf8` sources (`text`, `varchar`, `bpchar`, `name`, `json`,
/// `xml`) have a binary send form that already *is* UTF-8 text. A few
/// Postgres types map to Arrow strings but send non-text binary — `uuid`,
/// `inet`, `cidr`, `macaddr` — so we format those to the exact text the
/// `::text` bootstrap path (and SQL queries) produce, keeping snapshot and WAL
/// in agreement. Any other OID targeting a text column is an explicit error
/// rather than a silent mis-decode.
fn decode_binary_text(raw: &[u8], type_oid: u32) -> Result<Cow<'_, str>> {
    use postgres_protocol::types as pg;

    let decode_err =
        move |e: Box<dyn std::error::Error + Sync + Send>| super::Error::PgOutputDecode {
            message: format!(
                "postgres_replication: binary text decode failed (type oid {type_oid}): {e}"
            ),
        };

    match type_oid {
        // text, varchar, bpchar, name, json, xml — binary send is UTF-8 text.
        25 | 1043 | 1042 | 19 | 114 | 142 => {
            Ok(Cow::Borrowed(pg::text_from_sql(raw).map_err(decode_err)?))
        }
        // uuid → canonical lowercase hyphenated form.
        2950 => Ok(Cow::Owned(format_uuid(
            &pg::uuid_from_sql(raw).map_err(decode_err)?,
        ))),
        // macaddr → lowercase colon-separated form.
        829 => Ok(Cow::Owned(format_macaddr(
            pg::macaddr_from_sql(raw).map_err(decode_err)?,
        ))),
        // inet / cidr → `addr` or `addr/bits` (matches inet_out / cidr_out).
        869 | 650 => Ok(Cow::Owned(format_inet(raw)?)),
        other => PgOutputDecodeSnafu {
            message: format!(
                "postgres_replication: binary decoding into a text column is not supported for \
                 Postgres type OID {other}. Exclude the column from the dataset schema, or \
                 request text replication output for this dataset."
            ),
        }
        .fail(),
    }
}

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

fn push_hex_byte(s: &mut String, byte: u8) {
    s.push(HEX_LOWER[(byte >> 4) as usize] as char);
    s.push(HEX_LOWER[(byte & 0x0f) as usize] as char);
}

/// Format 16 UUID bytes as `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (lowercase),
/// matching Postgres `uuid_out`.
fn format_uuid(bytes: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        push_hex_byte(&mut s, *byte);
    }
    s
}

/// Format 6 MAC bytes as `xx:xx:xx:xx:xx:xx` (lowercase), matching Postgres
/// `macaddr_out`.
fn format_macaddr(bytes: [u8; 6]) -> String {
    let mut s = String::with_capacity(17);
    for (i, byte) in bytes.iter().enumerate() {
        if i != 0 {
            s.push(':');
        }
        push_hex_byte(&mut s, *byte);
    }
    s
}

/// Format a binary `inet`/`cidr` as Postgres would: `addr/bits` always for
/// `cidr`, and for `inet` only when `bits` is not the address width (matching
/// `inet_out`/`cidr_out`). IP address text uses the standard canonical form
/// (RFC 5952 for IPv6).
fn format_inet(raw: &[u8]) -> Result<String> {
    use postgres_protocol::types as pg;

    let inet = pg::inet_from_sql(raw).map_err(|e| super::Error::PgOutputDecode {
        message: format!("postgres_replication: binary inet decode failed: {e}"),
    })?;
    // Byte 2 of the wire format is the `is_cidr` flag, which `Inet` discards but
    // which decides whether a full-width prefix is printed.
    let is_cidr = raw.get(2).is_some_and(|b| *b != 0);
    let addr = inet.addr();
    let bits = inet.netmask();
    let max_bits = if addr.is_ipv4() { 32 } else { 128 };
    Ok(if is_cidr || bits != max_bits {
        format!("{addr}/{bits}")
    } else {
        format!("{addr}")
    })
}

fn parse_pg_numeric_to_i128(s: &str, precision: u8, scale: i8) -> Result<i128> {
    let trimmed = s.trim();
    if trimmed.eq_ignore_ascii_case("nan")
        || trimmed.eq_ignore_ascii_case("infinity")
        || trimmed.eq_ignore_ascii_case("-infinity")
    {
        return PgOutputDecodeSnafu {
            message: format!("numeric value '{trimmed}' is not representable as Decimal128"),
        }
        .fail();
    }

    // Normalise scientific notation via f-string-like expansion.
    let expanded = expand_scientific_notation(trimmed)?;

    let (sign, rest) = match expanded.strip_prefix('-') {
        Some(r) => (-1i128, r.to_string()),
        None => (1i128, expanded.trim_start_matches('+').to_string()),
    };

    // Split on decimal point.
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i.to_string(), f.to_string()),
        None => (rest, String::new()),
    };

    // Adjust fractional part to match the Arrow scale.
    let target_scale = usize::try_from(scale.max(0)).unwrap_or(0);
    let mut frac = frac_part;
    if frac.len() > target_scale {
        return PgOutputDecodeSnafu {
            message: format!(
                "numeric value '{s}' has scale {} but dataset schema declares scale {scale}",
                frac.len()
            ),
        }
        .fail();
    }
    while frac.len() < target_scale {
        frac.push('0');
    }

    let combined = format!("{int_part}{frac}");
    let magnitude: i128 = combined.parse().map_err(|e| super::Error::PgOutputDecode {
        message: format!("numeric '{s}' parse to i128: {e}"),
    })?;
    let value = sign * magnitude;

    // Enforce the declared precision here (with a friendly error) rather than
    // relying on Arrow, and to stay consistent with the binary decoder.
    ensure_decimal_precision(value, precision)?;
    Ok(value)
}

/// Expand scientific-notation NUMERIC text to its decimal form WITHOUT going
/// through floating-point. Preserves precision up to the full Decimal128 range.
fn expand_scientific_notation(s: &str) -> Result<String> {
    // Fast-path: no 'e' / 'E'.
    let Some(e_idx) = s.find(['e', 'E']) else {
        return Ok(s.to_string());
    };
    let significand = &s[..e_idx];
    let exponent_str = &s[e_idx + 1..];
    if significand.is_empty() {
        return sci_err(s, "missing significand");
    }
    if exponent_str.is_empty() {
        return sci_err(s, "missing exponent");
    }
    let exponent: i64 = exponent_str
        .parse()
        .map_err(|e| scientific_notation_numeric_error(s, &format!("invalid exponent: {e}")))?;

    let (sign, unsigned) = match significand.as_bytes().first() {
        Some(b'+') => ("", &significand[1..]),
        Some(b'-') => ("-", &significand[1..]),
        _ => ("", significand),
    };
    if unsigned.is_empty() {
        return sci_err(s, "missing digits in significand");
    }

    let mut digits = String::with_capacity(unsigned.len());
    let mut seen_decimal = false;
    let mut seen_digit = false;
    let mut fractional_digits: i64 = 0;
    for ch in unsigned.chars() {
        match ch {
            '0'..='9' => {
                digits.push(ch);
                seen_digit = true;
                if seen_decimal {
                    fractional_digits += 1;
                }
            }
            '.' if !seen_decimal => seen_decimal = true,
            '.' => return sci_err(s, "multiple decimal points in significand"),
            _ => return sci_err(s, "invalid character in significand"),
        }
    }
    if !seen_digit {
        return sci_err(s, "missing digits in significand");
    }
    if digits.bytes().all(|b| b == b'0') {
        return Ok("0".to_string());
    }

    let decimal_shift = exponent - fractional_digits;
    if decimal_shift >= 0 {
        let zero_count: usize = decimal_shift
            .try_into()
            .map_err(|_| scientific_notation_numeric_error(s, "exponent is too large to expand"))?;
        let mut out = String::with_capacity(sign.len() + digits.len() + zero_count);
        out.push_str(sign);
        out.push_str(&digits);
        out.extend(std::iter::repeat_n('0', zero_count));
        return Ok(out);
    }

    let split_pos = i64::try_from(digits.len())
        .map_err(|_| scientific_notation_numeric_error(s, "significand is too large to expand"))?
        + decimal_shift;

    if split_pos > 0 {
        let split_index: usize = split_pos.try_into().map_err(|_| {
            scientific_notation_numeric_error(s, "expanded decimal point is out of range")
        })?;
        let mut out = String::with_capacity(sign.len() + digits.len() + 1);
        out.push_str(sign);
        out.push_str(&digits[..split_index]);
        out.push('.');
        out.push_str(&digits[split_index..]);
        return Ok(out);
    }

    let leading_zero_count: usize = (-split_pos)
        .try_into()
        .map_err(|_| scientific_notation_numeric_error(s, "exponent is too small to expand"))?;
    let mut out = String::with_capacity(sign.len() + 2 + leading_zero_count + digits.len());
    out.push_str(sign);
    out.push_str("0.");
    out.extend(std::iter::repeat_n('0', leading_zero_count));
    out.push_str(&digits);
    Ok(out)
}

fn sci_err(s: &str, reason: &str) -> Result<String> {
    Err(scientific_notation_numeric_error(s, reason))
}

fn scientific_notation_numeric_error(s: &str, reason: &str) -> super::Error {
    super::Error::PgOutputDecode {
        message: format!("scientific-notation numeric parse '{s}': {reason}"),
    }
}

fn decode_hex(hex: &str) -> std::result::Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("odd number of hex digits".to_string());
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let h = hex_digit(pair[0])?;
        let l = hex_digit(pair[1])?;
        out.push((h << 4) | l);
    }
    Ok(out)
}

fn hex_digit(b: u8) -> std::result::Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("invalid hex digit: {b:#x}")),
    }
}

fn parse_pg_timestamp_micros(s: &str) -> Result<i64> {
    // Try with timezone, then without.
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Ok(dt.timestamp_micros());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    PgOutputDecodeSnafu {
        message: format!("timestamp parse '{s}' failed"),
    }
    .fail()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_replication::pgoutput::{Column as PgColumn, Value as PgValue};
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn lsn_committer_supports_deferral() {
        // The Postgres replication slot retains WAL until `confirmed_flush`
        // advances; deferring this commit holds the slot back, so a crash before the
        // deferred commit re-streams the un-acked tail (exactly-once via the
        // idempotent apply). So the LSN committer is the one committer an in-memory
        // durability tier is allowed to arm/defer on.
        let committer = LsnCommitter {
            confirmed_flush: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            flush_to: 42,
            dataset: "test".to_string(),
            source_commit_ts_ms: None,
        };
        assert!(committer.supports_deferral());
    }

    fn make_relation() -> Relation {
        Relation {
            relation_id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: b'd',
            columns: vec![
                PgColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                PgColumn {
                    is_key: false,
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }

    fn tuple_for(id: &str, name: Option<&str>) -> TupleData {
        TupleData {
            columns: vec![
                Some(PgValue::Text(bytes::Bytes::from(id.to_string()))),
                name.map(|n| PgValue::Text(bytes::Bytes::from(n.to_string()))),
            ],
        }
    }

    #[test]
    fn build_batch_with_insert_and_delete() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let relation = make_relation();
        let changes = vec![
            DecodedChange {
                op: ChangeOp::Create,
                row: tuple_for("1", Some("Alice")),
            },
            DecodedChange {
                op: ChangeOp::Delete,
                row: tuple_for("2", None),
            },
        ];
        let batch = build_change_batch(&schema, &relation, &changes).expect("build batch");
        assert_eq!(batch.record.num_rows(), 2);

        // op column
        let ops = batch
            .record
            .column_by_name("op")
            .expect("op")
            .as_string::<i32>();
        assert_eq!(ops.value(0), "c");
        assert_eq!(ops.value(1), "d");

        // primary_keys list
        let pks = batch
            .record
            .column_by_name("primary_keys")
            .expect("pks")
            .as_list::<i32>();
        let first = pks.value(0);
        assert_eq!(first.as_string::<i32>().value(0), "id");

        // data struct — id column
        let data = batch.record.column_by_name("data").expect("data");
        let data = data.as_struct();
        let id_col = data
            .column_by_name("id")
            .expect("id")
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);

        let name_col = data
            .column_by_name("name")
            .expect("name")
            .as_string::<i32>();
        assert_eq!(name_col.value(0), "Alice");
        assert!(name_col.is_null(1));
    }

    #[test]
    fn build_change_batch_memory_sized_to_row_count() {
        // Regression guard: the data-struct builders must be sized to the
        // transaction's row count. Default-capacity Arrow builders reserve
        // 1024 elements per column, so a 1-row change on a wide schema both
        // allocated and reported ~100 KB from `get_array_memory_size()`,
        // inflating the CDC coalescer's byte budget and the Cayenne mem-tier
        // accounting by orders of magnitude.
        let mut fields = vec![Field::new("id", DataType::Int32, false)];
        let mut columns = vec![PgColumn {
            is_key: true,
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
        }];
        for i in 0..12 {
            let name = format!("v{i}");
            fields.push(Field::new(&name, DataType::Int64, true));
            columns.push(PgColumn {
                is_key: false,
                name,
                type_oid: 20,
                type_modifier: -1,
            });
        }
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let relation = Relation {
            relation_id: 1,
            namespace: "public".to_string(),
            name: "wide".to_string(),
            replica_identity: b'd',
            columns,
        };
        let row = TupleData {
            columns: (0..13)
                .map(|i| Some(PgValue::Text(bytes::Bytes::from(i.to_string()))))
                .collect(),
        };
        let changes = vec![DecodedChange {
            op: ChangeOp::Create,
            row,
        }];

        let batch = build_change_batch(&schema, &relation, &changes).expect("build batch");
        assert_eq!(batch.record.num_rows(), 1);
        let size = batch.record.get_array_memory_size();
        assert!(
            size < 16 * 1024,
            "1-row change batch reports {size} bytes; data builders are likely \
             no longer sized to num_rows (default-capacity Arrow builders \
             reserve 1024 elements per column)"
        );
    }

    #[tokio::test]
    async fn lsn_committer_advances_monotonically() {
        let lsn = Arc::new(AtomicU64::new(0));
        let c1 = LsnCommitter {
            confirmed_flush: Arc::clone(&lsn),
            flush_to: 100,
            dataset: "test".to_string(),
            source_commit_ts_ms: None,
        };
        c1.commit().await.expect("commit");
        assert_eq!(lsn.load(std::sync::atomic::Ordering::Relaxed), 100);

        // older commit should not regress.
        let c2 = LsnCommitter {
            confirmed_flush: Arc::clone(&lsn),
            flush_to: 50,
            dataset: "test".to_string(),
            source_commit_ts_ms: None,
        };
        c2.commit().await.expect("commit");
        assert_eq!(lsn.load(std::sync::atomic::Ordering::Relaxed), 100);
    }

    // ---------------------------------------------------------------------
    // Comprehensive operation + nullability tests.
    //
    // These cover the four pgoutput ops (INSERT / UPDATE / DELETE / TRUNCATE)
    // on tables with both nullable and NOT NULL columns, in the standard
    // REPLICA IDENTITY DEFAULT (only PK in old tuple) and REPLICA IDENTITY
    // FULL (all columns in old tuple) shapes.
    // ---------------------------------------------------------------------

    fn insert_change(id: &str, name: Option<&str>) -> DecodedChange {
        DecodedChange {
            op: ChangeOp::Create,
            row: tuple_for(id, name),
        }
    }

    fn update_change(id: &str, name: Option<&str>) -> DecodedChange {
        DecodedChange {
            op: ChangeOp::Update,
            row: tuple_for(id, name),
        }
    }

    fn delete_change_default_identity(id: &str) -> DecodedChange {
        // REPLICA IDENTITY DEFAULT — K tuple has all relation columns, but
        // only PKs are populated; non-PK columns are explicitly null.
        DecodedChange {
            op: ChangeOp::Delete,
            row: tuple_for(id, None),
        }
    }

    fn delete_change_full_identity(id: &str, name: Option<&str>) -> DecodedChange {
        // REPLICA IDENTITY FULL — O tuple has all columns populated.
        DecodedChange {
            op: ChangeOp::Delete,
            row: tuple_for(id, name),
        }
    }

    fn truncate_change() -> DecodedChange {
        DecodedChange {
            op: ChangeOp::Truncate,
            row: TupleData { columns: vec![] },
        }
    }

    fn assert_op_column(batch: &ChangeBatch, expected_ops: &[&str]) {
        let ops = batch
            .record
            .column_by_name("op")
            .expect("op")
            .as_string::<i32>();
        assert_eq!(ops.len(), expected_ops.len(), "op column length");
        for (i, want) in expected_ops.iter().enumerate() {
            assert_eq!(ops.value(i), *want, "row {i} op");
        }
    }

    fn id_value(batch: &ChangeBatch, row: usize) -> i32 {
        batch
            .record
            .column_by_name("data")
            .expect("data")
            .as_struct()
            .column_by_name("id")
            .expect("id")
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(row)
    }

    fn name_is_null(batch: &ChangeBatch, row: usize) -> bool {
        batch
            .record
            .column_by_name("data")
            .expect("data")
            .as_struct()
            .column_by_name("name")
            .expect("name")
            .as_string::<i32>()
            .is_null(row)
    }

    /// NOT NULL name column is the scenario where the original bug bites:
    /// DELETE with REPLICA IDENTITY DEFAULT sends name=null, and
    /// `StructArray::new` would reject that unless we intentionally relax the
    /// field's nullability when assembling the `ChangeBatch`.
    fn non_nullable_users_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false), // NOT NULL
        ]))
    }

    fn nullable_users_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn insert_populates_all_columns() {
        for schema in [nullable_users_schema(), non_nullable_users_schema()] {
            let batch = build_change_batch(
                &schema,
                &make_relation(),
                &[insert_change("42", Some("Charlie"))],
            )
            .expect("build");
            assert_op_column(&batch, &["c"]);
            assert_eq!(id_value(&batch, 0), 42);
            assert!(!name_is_null(&batch, 0));
        }
    }

    #[test]
    fn update_uses_new_tuple_values() {
        for schema in [nullable_users_schema(), non_nullable_users_schema()] {
            let batch = build_change_batch(
                &schema,
                &make_relation(),
                &[update_change("1", Some("Updated"))],
            )
            .expect("build");
            assert_op_column(&batch, &["u"]);
            assert_eq!(id_value(&batch, 0), 1);
            assert!(!name_is_null(&batch, 0));
        }
    }

    #[test]
    fn push_update_change_keeps_non_pk_update_as_single_upsert() {
        let relation = make_relation();
        let mut changes = Vec::new();
        push_update_change(
            &mut changes,
            &relation,
            Some(tuple_for("1", Some("Old"))),
            tuple_for("1", Some("Updated")),
        );

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].op, ChangeOp::Update);
        assert_eq!(changes[0].row.columns[0], Some(PgValue::Text("1".into())));
    }

    #[test]
    fn push_update_change_emits_old_key_delete_for_pk_update() {
        let relation = make_relation();
        let mut changes = Vec::new();
        push_update_change(
            &mut changes,
            &relation,
            Some(tuple_for("1", None)),
            tuple_for("1001", Some("Updated")),
        );

        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].op, ChangeOp::Delete);
        assert_eq!(changes[0].row.columns[0], Some(PgValue::Text("1".into())));
        assert_eq!(changes[1].op, ChangeOp::Update);
        assert_eq!(
            changes[1].row.columns[0],
            Some(PgValue::Text("1001".into()))
        );
    }

    #[test]
    fn primary_key_update_change_batch_is_delete_then_update() {
        let schema = non_nullable_users_schema();
        let relation = make_relation();
        let mut changes = Vec::new();
        push_update_change(
            &mut changes,
            &relation,
            Some(tuple_for("1", None)),
            tuple_for("1001", Some("Updated")),
        );

        let batch = build_change_batch(&schema, &relation, &changes).expect("build");
        assert_op_column(&batch, &["d", "u"]);
        assert_eq!(id_value(&batch, 0), 1);
        assert_eq!(id_value(&batch, 1), 1001);
        assert!(name_is_null(&batch, 0));
        assert!(!name_is_null(&batch, 1));
    }

    #[test]
    fn delete_default_identity_succeeds_on_non_null_schema() {
        // This is the regression: previously failed on non-null schemas
        // because DELETE sends name=null and StructArray::new rejected it.
        let schema = non_nullable_users_schema();
        let batch = build_change_batch(
            &schema,
            &make_relation(),
            &[delete_change_default_identity("7")],
        )
        .expect(
            "DELETE on non-null schema must succeed — data struct is always built \
                         with nullable fields to hold the null-padded old tuple",
        );
        assert_op_column(&batch, &["d"]);
        assert_eq!(id_value(&batch, 0), 7);
        assert!(name_is_null(&batch, 0));
    }

    #[test]
    fn delete_full_identity_populates_all_columns() {
        let schema = non_nullable_users_schema();
        let batch = build_change_batch(
            &schema,
            &make_relation(),
            &[delete_change_full_identity("9", Some("DelName"))],
        )
        .expect("build");
        assert_op_column(&batch, &["d"]);
        assert_eq!(id_value(&batch, 0), 9);
        assert!(!name_is_null(&batch, 0));
    }

    #[test]
    fn truncate_produces_empty_row_entry() {
        let schema = non_nullable_users_schema();
        let batch = build_change_batch(&schema, &make_relation(), &[truncate_change()])
            .expect("truncate build");
        assert_op_column(&batch, &["t"]);
        // row columns are empty for truncate → all null after nullable-override.
        assert!(name_is_null(&batch, 0));
    }

    #[test]
    fn mixed_ops_in_one_transaction_preserve_order() {
        let schema = non_nullable_users_schema();
        let batch = build_change_batch(
            &schema,
            &make_relation(),
            &[
                insert_change("1", Some("A")),
                update_change("1", Some("A1")),
                delete_change_default_identity("1"),
            ],
        )
        .expect("build");
        assert_op_column(&batch, &["c", "u", "d"]);
    }

    #[test]
    fn composite_primary_key_populates_both_pks() {
        // Composite PK: (tenant, id). Relation + schema have both as keys.
        let rel = Relation {
            relation_id: 2,
            namespace: "public".into(),
            name: "composite".into(),
            replica_identity: b'd',
            columns: vec![
                PgColumn {
                    is_key: true,
                    name: "tenant".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                PgColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                PgColumn {
                    is_key: false,
                    name: "label".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        // DELETE sends tenant+id populated, label null.
        let delete = DecodedChange {
            op: ChangeOp::Delete,
            row: TupleData {
                columns: vec![
                    Some(PgValue::Text("5".into())),
                    Some(PgValue::Text("99".into())),
                    None,
                ],
            },
        };
        let batch = build_change_batch(&schema, &rel, &[delete]).expect("build");
        assert_op_column(&batch, &["d"]);

        let pks = batch
            .record
            .column_by_name("primary_keys")
            .expect("pks")
            .as_list::<i32>();
        let first = pks.value(0);
        let pk_strs = first.as_string::<i32>();
        assert_eq!(pk_strs.len(), 2);
        assert_eq!(pk_strs.value(0), "tenant");
        assert_eq!(pk_strs.value(1), "id");
    }

    // ------------ Type coverage ------------

    fn single_col_relation(name: &str) -> Relation {
        Relation {
            relation_id: 3,
            namespace: "public".into(),
            name: "t".into(),
            replica_identity: b'd',
            columns: vec![PgColumn {
                is_key: true,
                name: name.to_string(),
                type_oid: 0,
                type_modifier: -1,
            }],
        }
    }

    fn single_col_change(op: ChangeOp, _name: &str, text: &str) -> DecodedChange {
        DecodedChange {
            op,
            row: TupleData {
                columns: vec![Some(PgValue::Text(bytes::Bytes::from(text.to_string())))],
            },
        }
    }

    #[test]
    fn fieldbuilder_parses_integers() {
        let rel = single_col_relation("v");
        for (dt, text, probe) in [
            (DataType::Int8, "-5", "-5"),
            (DataType::Int16, "32000", "32000"),
            (DataType::Int32, "-12345", "-12345"),
            (DataType::Int64, "9999999999", "9999999999"),
            (DataType::UInt32, "4294967290", "4294967290"),
        ] {
            let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", dt.clone(), true)]));
            let batch = build_change_batch(
                &schema,
                &rel,
                &[single_col_change(ChangeOp::Create, "v", text)],
            )
            .unwrap_or_else(|e| panic!("build {dt}: {e}"));
            let col = batch
                .record
                .column_by_name("data")
                .expect("data")
                .as_struct()
                .column_by_name("v")
                .expect("v");
            assert!(!col.is_null(0), "row should not be null for {dt} = {probe}");
        }
    }

    #[test]
    fn fieldbuilder_parses_floats() {
        let rel = single_col_relation("v");
        for (dt, text) in [(DataType::Float32, "1.5"), (DataType::Float64, "2.75")] {
            let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", dt.clone(), true)]));
            build_change_batch(
                &schema,
                &rel,
                &[single_col_change(ChangeOp::Create, "v", text)],
            )
            .unwrap_or_else(|e| panic!("build {dt}: {e}"));
        }
    }

    #[test]
    fn fieldbuilder_parses_bool_date_time_timestamps() {
        let rel = single_col_relation("v");
        let cases: Vec<(DataType, &str)> = vec![
            (DataType::Boolean, "t"),
            (DataType::Date32, "1996-01-02"),
            (DataType::Time64(TimeUnit::Nanosecond), "12:34:56.789"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "2024-01-02 03:04:05.678",
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "2024-01-02 03:04:05.678901",
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                "2024-01-02 03:04:05.678+00",
            ),
        ];
        for (dt, text) in cases {
            let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", dt.clone(), true)]));
            build_change_batch(
                &schema,
                &rel,
                &[single_col_change(ChangeOp::Create, "v", text)],
            )
            .unwrap_or_else(|e| panic!("build {dt}: {e}"));
        }
    }

    #[test]
    fn fieldbuilder_parses_bytea_hex() {
        let rel = single_col_relation("v");
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Binary, true)]));
        let batch = build_change_batch(
            &schema,
            &rel,
            &[single_col_change(ChangeOp::Create, "v", "\\xdeadbeef")],
        )
        .expect("build");
        let col = batch
            .record
            .column_by_name("data")
            .expect("data")
            .as_struct()
            .column_by_name("v")
            .expect("v");
        let bin = col
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("binary");
        assert_eq!(bin.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn fieldbuilder_parses_decimal_with_scale() {
        let rel = single_col_relation("v");
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(38, 2),
            true,
        )]));
        // 123.45 with scale 2 → i128 = 12345
        let batch = build_change_batch(
            &schema,
            &rel,
            &[single_col_change(ChangeOp::Create, "v", "123.45")],
        )
        .expect("build");
        let col = batch
            .record
            .column_by_name("data")
            .expect("data")
            .as_struct()
            .column_by_name("v")
            .expect("v");
        let dec = col
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .expect("decimal128");
        assert_eq!(dec.value(0), 12345i128);
    }

    #[test]
    fn fieldbuilder_rejects_nested_arrays_with_actionable_error() {
        // Single-level arrays are supported; multidimensional ones are not.
        let nested = DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )));
        let Err(err) = FieldBuilder::with_capacity(&nested, 1) else {
            panic!("expected nested-List rejection");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("multidimensional arrays are not supported"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn pg_array_literal_parses_standard_cases() {
        assert_eq!(
            parse_pg_array_literal("{}").expect("empty"),
            Vec::<Option<String>>::new()
        );
        assert_eq!(
            parse_pg_array_literal("{a,b,c}").expect("plain"),
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string())
            ]
        );
        // Quoted elements: embedded commas, escaped quotes/backslashes, and a
        // literal "NULL" string (quoted ⇒ not a NULL element).
        assert_eq!(
            parse_pg_array_literal(r#"{"b c","quo\"te","back\\slash",NULL,"NULL"}"#)
                .expect("quoted"),
            vec![
                Some("b c".to_string()),
                Some("quo\"te".to_string()),
                Some("back\\slash".to_string()),
                None,
                Some("NULL".to_string())
            ]
        );
        // Dimension-bounds prefix.
        assert_eq!(
            parse_pg_array_literal("[0:1]={x,y}").expect("bounds"),
            vec![Some("x".to_string()), Some("y".to_string())]
        );
    }

    #[test]
    fn pg_array_literal_rejects_malformed() {
        for bad in [
            "not-an-array",
            "{unterminated",
            r#"{"open}"#,
            "{{1,2},{3,4}}",
        ] {
            parse_pg_array_literal(bad).expect_err(bad);
        }
    }

    #[test]
    fn fieldbuilder_builds_text_and_int_arrays() {
        let rel = single_col_relation("v");
        // text[] → List(Utf8)
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));
        let batch = build_change_batch(
            &schema,
            &rel,
            &[single_col_change(
                ChangeOp::Create,
                "v",
                r#"{red,"b c",NULL}"#,
            )],
        )
        .expect("build text[]");
        let col = batch
            .record
            .column_by_name("data")
            .expect("data")
            .as_struct()
            .column_by_name("v")
            .expect("v")
            .as_list::<i32>()
            .value(0);
        let items = col.as_string::<i32>();
        assert_eq!(items.value(0), "red");
        assert_eq!(items.value(1), "b c");
        assert!(items.is_null(2));

        // int4[] → List(Int32)
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));
        let batch = build_change_batch(
            &schema,
            &rel,
            &[
                single_col_change(ChangeOp::Create, "v", "{1,2,3}"),
                // NULL array (whole column value).
                DecodedChange {
                    op: ChangeOp::Create,
                    row: TupleData {
                        columns: vec![None],
                    },
                },
            ],
        )
        .expect("build int[]");
        let lists = batch
            .record
            .column_by_name("data")
            .expect("data")
            .as_struct()
            .column_by_name("v")
            .expect("v")
            .as_list::<i32>()
            .clone();
        let first = lists.value(0);
        let ints = first.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(ints.values(), &[1, 2, 3]);
        assert!(lists.is_null(1), "NULL array value must be a null list");
    }

    #[test]
    fn fieldbuilder_builds_dictionary_enum_column() {
        // Postgres ENUM columns map to Dictionary(Int8, Utf8) in the dataset
        // schema; pgoutput delivers the enum's text label.
        let rel = single_col_relation("v");
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )]));
        let batch = build_change_batch(
            &schema,
            &rel,
            &[
                single_col_change(ChangeOp::Create, "v", "active"),
                single_col_change(ChangeOp::Create, "v", "paused"),
                DecodedChange {
                    op: ChangeOp::Create,
                    row: TupleData {
                        columns: vec![None],
                    },
                },
                single_col_change(ChangeOp::Create, "v", "active"),
            ],
        )
        .expect("build enum dictionary");
        let dict = batch
            .record
            .column_by_name("data")
            .expect("data")
            .as_struct()
            .column_by_name("v")
            .expect("v")
            .as_dictionary::<Int8Type>()
            .clone();
        let values = dict.values().as_string::<i32>();
        assert_eq!(values.value(dict.key(0).expect("key 0")), "active");
        assert_eq!(values.value(dict.key(1).expect("key 1")), "paused");
        assert!(dict.is_null(2));
        assert_eq!(values.value(dict.key(3).expect("key 3")), "active");
    }

    #[test]
    fn fieldbuilder_rejects_interval() {
        let Err(err) = FieldBuilder::with_capacity(
            &DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
            1,
        ) else {
            panic!("expected Interval rejection");
        };
        assert!(err.to_string().contains("INTERVAL"));
    }

    #[test]
    fn unchanged_toast_merges_from_old_tuple_under_replica_identity_full() {
        // REPLICA IDENTITY FULL: the UPDATE's old tuple carries the real
        // value of the unchanged TOASTed column; the merge substitutes it.
        let new = TupleData {
            columns: vec![Some(PgValue::Text("1".into())), Some(PgValue::Unchanged)],
        };
        let old = TupleData {
            columns: vec![
                Some(PgValue::Text("1".into())),
                Some(PgValue::Text("big toasted value".into())),
            ],
        };
        let merged = merge_unchanged_toast(new, Some(&old));
        assert!(
            matches!(&merged.columns[1], Some(PgValue::Text(s)) if s == "big toasted value"),
            "unchanged TOAST column must take the old tuple's value"
        );

        // And the merged tuple builds a valid change batch.
        let schema = non_nullable_users_schema();
        let batch = build_change_batch(
            &schema,
            &make_relation(),
            &[DecodedChange {
                op: ChangeOp::Update,
                row: merged,
            }],
        )
        .expect("merged update must build");
        assert_op_column(&batch, &["u"]);

        // Old tuple with a NULL in that slot (or no old tuple at all) leaves
        // the marker untouched/NULL appropriately.
        let new = TupleData {
            columns: vec![Some(PgValue::Text("1".into())), Some(PgValue::Unchanged)],
        };
        let untouched = merge_unchanged_toast(new, None);
        assert!(matches!(&untouched.columns[1], Some(PgValue::Unchanged)));
    }

    #[test]
    fn unchanged_toast_during_update_errors_clearly() {
        let rel = make_relation();
        let schema = non_nullable_users_schema();
        let unchanged_update = DecodedChange {
            op: ChangeOp::Update,
            row: TupleData {
                columns: vec![Some(PgValue::Text("1".into())), Some(PgValue::Unchanged)],
            },
        };
        let err = build_change_batch(&schema, &rel, &[unchanged_update])
            .expect_err("Value::Unchanged during UPDATE must error");
        let msg = err.to_string();
        assert!(
            msg.contains("REPLICA IDENTITY FULL"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn numeric_parser_handles_standard_cases() {
        // Scale 2, value 123.45 → 12345
        assert_eq!(
            parse_pg_numeric_public("123.45", 2).expect("parse 123.45"),
            12_345i128
        );
        // Negative
        assert_eq!(
            parse_pg_numeric_public("-7.25", 2).expect("parse -7.25"),
            -725i128
        );
        // Integer (no decimal point) with scale 2 → padded
        assert_eq!(parse_pg_numeric_public("7", 2).expect("parse 7"), 700i128);
        // Explicit "+" sign
        assert_eq!(
            parse_pg_numeric_public("+1.5", 2).expect("parse +1.5"),
            150i128
        );
        // Zero
        assert_eq!(
            parse_pg_numeric_public("0.00", 2).expect("parse 0.00"),
            0i128
        );
    }

    #[test]
    fn numeric_parser_rejects_nan_and_inf() {
        for bad in ["NaN", "Infinity", "-Infinity"] {
            let err = parse_pg_numeric_public(bad, 2).expect_err(bad);
            assert!(err.to_string().contains("not representable"));
        }
    }

    #[test]
    fn numeric_parser_rejects_overscale() {
        // 0.1234 with scale 2 has 4 fractional digits → error, not silent truncation.
        let err = parse_pg_numeric_public("0.1234", 2).expect_err("should reject");
        assert!(err.to_string().contains("scale"));
    }

    #[test]
    fn hex_decoder_round_trips() {
        assert_eq!(
            decode_hex("deadbeef").expect("decode deadbeef"),
            vec![0xde, 0xad, 0xbe, 0xef]
        );
        assert_eq!(decode_hex("").expect("decode empty"), Vec::<u8>::new());
        // Odd length → error.
        decode_hex("abc").expect_err("odd length should fail");
        // Invalid digit → error.
        decode_hex("zz").expect_err("invalid digit should fail");
    }

    // ---- binary-format (pgoutput `b` tag) decode tests ----------------------

    use arrow::datatypes::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, Int64Type, Time64NanosecondType,
        TimestampNanosecondType, UInt32Type,
    };
    use bytes::Bytes;

    /// Append one binary (`send`-format) value into a fresh builder for `dt`
    /// and finish it into a single-element array.
    fn bin_one(dt: &DataType, type_oid: u32, raw: &[u8]) -> ArrayRef {
        let mut fb = FieldBuilder::with_capacity(dt, 1).expect("builder");
        fb.append(
            Some(&PgValue::Binary(Bytes::copy_from_slice(raw))),
            ChangeOp::Create,
            type_oid,
        )
        .expect("append binary value");
        fb.finish()
    }

    /// Encode a Postgres binary `numeric` from its base-10000 digit groups.
    fn enc_numeric(digits: &[u16], weight: i16, negative: bool, dscale: u16) -> Vec<u8> {
        let mut o = Vec::new();
        o.extend_from_slice(&(u16::try_from(digits.len()).expect("ndigits")).to_be_bytes());
        o.extend_from_slice(&weight.to_be_bytes());
        o.extend_from_slice(&(if negative { 0x4000u16 } else { 0 }).to_be_bytes());
        o.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            o.extend_from_slice(&d.to_be_bytes());
        }
        o
    }

    #[test]
    fn binary_scalar_types_decode() {
        assert!(bin_one(&DataType::Boolean, 16, &[1]).as_boolean().value(0));
        assert!(!bin_one(&DataType::Boolean, 16, &[0]).as_boolean().value(0));
        assert_eq!(
            bin_one(&DataType::Int16, 21, &1234i16.to_be_bytes())
                .as_primitive::<Int16Type>()
                .value(0),
            1234
        );
        assert_eq!(
            bin_one(&DataType::Int32, 23, &(-42i32).to_be_bytes())
                .as_primitive::<Int32Type>()
                .value(0),
            -42
        );
        assert_eq!(
            bin_one(&DataType::Int64, 20, &9_000_000_000i64.to_be_bytes())
                .as_primitive::<Int64Type>()
                .value(0),
            9_000_000_000
        );
        // "char" (oid 18) -> Int8; 0xFF is -1.
        assert_eq!(
            bin_one(&DataType::Int8, 18, &[0xFF])
                .as_primitive::<Int8Type>()
                .value(0),
            -1
        );
        assert_eq!(
            bin_one(&DataType::UInt32, 26, &4_000_000_000u32.to_be_bytes())
                .as_primitive::<UInt32Type>()
                .value(0),
            4_000_000_000
        );
        assert!(
            (bin_one(&DataType::Float32, 700, &1.5f32.to_be_bytes())
                .as_primitive::<Float32Type>()
                .value(0)
                - 1.5)
                .abs()
                < f32::EPSILON
        );
        assert!(
            (bin_one(&DataType::Float64, 701, &2.25f64.to_be_bytes())
                .as_primitive::<Float64Type>()
                .value(0)
                - 2.25)
                .abs()
                < f64::EPSILON
        );
        assert_eq!(
            bin_one(&DataType::Utf8, 25, b"hello")
                .as_string::<i32>()
                .value(0),
            "hello"
        );
        // bytea `send` form is identity.
        assert_eq!(
            bin_one(&DataType::Binary, 17, &[0xde, 0xad])
                .as_binary::<i32>()
                .value(0),
            &[0xde, 0xad]
        );
    }

    #[test]
    fn binary_temporal_decode() {
        // date: pg day 0 (2000-01-01) -> Arrow Date32 10957.
        assert_eq!(
            bin_one(&DataType::Date32, 1082, &0i32.to_be_bytes())
                .as_primitive::<Date32Type>()
                .value(0),
            10_957
        );
        // timestamp: pg micros 0 (2000-01-01) -> Arrow nanos since Unix epoch.
        let ts = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert_eq!(
            bin_one(&ts, 1114, &0i64.to_be_bytes())
                .as_primitive::<TimestampNanosecondType>()
                .value(0),
            946_684_800_000_000_000
        );
        // time: 1_000_000 micros since midnight (00:00:01) -> 1e9 nanos.
        let t = DataType::Time64(TimeUnit::Nanosecond);
        assert_eq!(
            bin_one(&t, 1083, &1_000_000i64.to_be_bytes())
                .as_primitive::<Time64NanosecondType>()
                .value(0),
            1_000_000_000
        );
    }

    #[test]
    fn binary_numeric_decode_matches_expected() {
        // 172799.49 @ scale 2 -> 17279949 (digits [17,2799,4900], weight 1).
        assert_eq!(
            numeric_from_binary(&enc_numeric(&[17, 2799, 4900], 1, false, 2), 15, 2)
                .expect("172799.49"),
            17_279_949
        );
        // 0.01 @ scale 2 -> 1.
        assert_eq!(
            numeric_from_binary(&enc_numeric(&[100], -1, false, 2), 15, 2).expect("0.01"),
            1
        );
        // 100 @ scale 2 -> 10000.
        assert_eq!(
            numeric_from_binary(&enc_numeric(&[100], 0, false, 2), 15, 2).expect("100.00"),
            10_000
        );
        // -5 @ scale 0 -> -5.
        assert_eq!(
            numeric_from_binary(&enc_numeric(&[5], 0, true, 0), 15, 0).expect("-5"),
            -5
        );
        // Zero (ndigits 0) -> 0.
        assert_eq!(
            numeric_from_binary(&enc_numeric(&[], 0, false, 0), 15, 2).expect("0"),
            0
        );
        // Same value through the Decimal128 builder arm of `append_binary`.
        assert_eq!(
            bin_one(
                &DataType::Decimal128(15, 2),
                1700,
                &enc_numeric(&[17, 2799, 4900], 1, false, 2)
            )
            .as_primitive::<Decimal128Type>()
            .value(0),
            17_279_949
        );
    }

    #[test]
    fn binary_numeric_rejects_overscale_and_special() {
        // 1.234 @ scale 2: more fractional precision than declared -> error, not
        // a silent round.
        numeric_from_binary(&enc_numeric(&[1, 2340], 0, false, 3), 15, 2)
            .expect_err("overscale must error");
        // NaN sign word 0xC000 is not representable as Decimal128.
        let mut nan = Vec::new();
        nan.extend_from_slice(&0u16.to_be_bytes()); // ndigits
        nan.extend_from_slice(&0i16.to_be_bytes()); // weight
        nan.extend_from_slice(&0xC000u16.to_be_bytes()); // sign = NaN
        nan.extend_from_slice(&0u16.to_be_bytes()); // dscale
        numeric_from_binary(&nan, 15, 2).expect_err("NaN must error");

        // 10^15 exceeds precision 15 (max unscaled magnitude 10^15 - 1) —
        // reject rather than store an out-of-precision Decimal128 value.
        // 10^15 = 1000 * 10000^3 → digits [1000], weight 3, scale 0.
        numeric_from_binary(&enc_numeric(&[1000], 3, false, 0), 15, 0)
            .expect_err("value exceeding declared precision must error");
        // One less (10^15 - 1) fits precision 15.
        assert_eq!(
            numeric_from_binary(&enc_numeric(&[999, 9999, 9999, 9999], 3, false, 0), 15, 0)
                .expect("10^15 - 1 fits precision 15"),
            999_999_999_999_999
        );
    }

    /// Encode a 1-D binary `int4[]` array (`send` wire form).
    fn enc_binary_int4_array(elems: &[Option<i32>]) -> Vec<u8> {
        let mut o = Vec::new();
        o.extend_from_slice(&1i32.to_be_bytes()); // ndim
        o.extend_from_slice(&1i32.to_be_bytes()); // flags (has nulls)
        o.extend_from_slice(&23u32.to_be_bytes()); // element oid = int4
        o.extend_from_slice(&(i32::try_from(elems.len()).expect("len")).to_be_bytes()); // dim len
        o.extend_from_slice(&1i32.to_be_bytes()); // lower bound
        for e in elems {
            match e {
                Some(v) => {
                    o.extend_from_slice(&4i32.to_be_bytes());
                    o.extend_from_slice(&v.to_be_bytes());
                }
                None => o.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        o
    }

    #[test]
    fn binary_array_int4_decode() {
        let dt = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let arr = bin_one(&dt, 1007, &enc_binary_int4_array(&[Some(1), None, Some(3)]));
        let list = arr.as_list::<i32>();
        assert_eq!(list.len(), 1);
        let values = list.value(0);
        let ints = values.as_primitive::<Int32Type>();
        assert_eq!(ints.len(), 3);
        assert_eq!(ints.value(0), 1);
        assert!(ints.is_null(1));
        assert_eq!(ints.value(2), 3);
    }

    #[test]
    fn binary_uuid_and_macaddr_decode_to_canonical_text() {
        // uuid → lowercase hyphenated (matches `uuid_out`).
        let uuid = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(
            bin_one(&DataType::Utf8, 2950, &uuid)
                .as_string::<i32>()
                .value(0),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        // macaddr → lowercase colon-separated (matches `macaddr_out`).
        assert_eq!(
            bin_one(&DataType::Utf8, 829, &[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03])
                .as_string::<i32>()
                .value(0),
            "08:00:2b:01:02:03"
        );
    }

    /// Encode a binary `inet`/`cidr` value.
    fn enc_inet(family: u8, bits: u8, is_cidr: u8, addr: &[u8]) -> Vec<u8> {
        let mut o = vec![
            family,
            bits,
            is_cidr,
            u8::try_from(addr.len()).expect("addr len fits u8"),
        ];
        o.extend_from_slice(addr);
        o
    }

    #[test]
    fn binary_inet_cidr_decode_to_canonical_text() {
        // inet host: full-width prefix omitted (matches `inet_out`).
        assert_eq!(
            bin_one(&DataType::Utf8, 869, &enc_inet(2, 32, 0, &[10, 0, 0, 1]))
                .as_string::<i32>()
                .value(0),
            "10.0.0.1"
        );
        // inet with a network prefix keeps it.
        assert_eq!(
            bin_one(&DataType::Utf8, 869, &enc_inet(2, 24, 0, &[10, 0, 0, 0]))
                .as_string::<i32>()
                .value(0),
            "10.0.0.0/24"
        );
        // cidr always prints the prefix, even at full width (matches `cidr_out`).
        assert_eq!(
            bin_one(&DataType::Utf8, 650, &enc_inet(2, 32, 1, &[10, 0, 0, 0]))
                .as_string::<i32>()
                .value(0),
            "10.0.0.0/32"
        );
        // IPv6 canonical (RFC 5952) compressed form.
        let mut v6 = [0u8; 16];
        v6[0] = 0x20;
        v6[1] = 0x01;
        v6[2] = 0x0d;
        v6[3] = 0xb8;
        v6[15] = 0x01;
        assert_eq!(
            bin_one(&DataType::Utf8, 869, &enc_inet(3, 128, 0, &v6))
                .as_string::<i32>()
                .value(0),
            "2001:db8::1"
        );
    }

    #[test]
    fn binary_text_column_rejects_unsupported_oid() {
        // An OID with no supported text/binary mapping targeting a Utf8 column
        // must error loudly (here: jsonb, whose binary carries a version byte),
        // never silently mis-decode into a wrong string.
        decode_binary_text(&[0x01, b'{', b'}'], 3802).expect_err("unsupported oid must error");
    }

    #[test]
    fn build_change_batch_decodes_binary_tuple() {
        // A row with binary-encoded columns flows through the same batch builder
        // as text, driven by the relation's per-column type OIDs.
        let relation = Relation {
            relation_id: 1,
            namespace: "public".to_string(),
            name: "orders".to_string(),
            replica_identity: b'd',
            columns: vec![
                PgColumn {
                    is_key: true,
                    name: "id".to_string(),
                    type_oid: 20,
                    type_modifier: -1,
                },
                PgColumn {
                    is_key: false,
                    name: "amount".to_string(),
                    type_oid: 1700,
                    type_modifier: -1,
                },
            ],
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(15, 2), false),
        ]));
        let change = DecodedChange {
            op: ChangeOp::Create,
            row: TupleData {
                columns: vec![
                    Some(PgValue::Binary(Bytes::from(7i64.to_be_bytes().to_vec()))),
                    Some(PgValue::Binary(Bytes::from(enc_numeric(
                        &[17, 2799, 4900],
                        1,
                        false,
                        2,
                    )))),
                ],
            },
        };
        let batch = build_change_batch(&schema, &relation, &[change]).expect("build batch");
        assert_eq!(batch.record.num_rows(), 1);
        let data = batch
            .record
            .column_by_name("data")
            .expect("data column")
            .as_struct();
        assert_eq!(
            data.column_by_name("id")
                .expect("id")
                .as_primitive::<Int64Type>()
                .value(0),
            7
        );
        assert_eq!(
            data.column_by_name("amount")
                .expect("amount")
                .as_primitive::<Decimal128Type>()
                .value(0),
            17_279_949
        );
    }
}

/// Differential tests for the deferred raw-buffering path (increment 2): the
/// shared pump buffers raw pgoutput change bytes and the per-dataset consumer
/// decodes them via [`decode_raw_changes`]. Each test asserts the raw path
/// yields a `ChangeBatch` byte-identical to the eager path (constructing the
/// `DecodedChange`s directly, as the pump used to do inline), so relocating the
/// tuple decode + TOAST/PK-split transform off the pump changed nothing observable.
#[cfg(test)]
mod raw_decode_tests {
    use super::*;
    use crate::postgres_replication::pgoutput::Column;
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use std::sync::Arc;

    fn relation() -> Arc<Relation> {
        Arc::new(Relation {
            relation_id: 1,
            namespace: "public".to_string(),
            name: "t".to_string(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    is_key: true,
                    name: "id".to_string(),
                    type_oid: 25, // text — keep typing trivial for the differential
                    type_modifier: -1,
                },
                Column {
                    is_key: false,
                    name: "v".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        })
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    fn tuple(vals: &[&str]) -> TupleData {
        TupleData {
            columns: vals
                .iter()
                .map(|s| Some(Value::Text(Bytes::copy_from_slice(s.as_bytes()))))
                .collect(),
        }
    }

    // ---- raw pgoutput message encoders (text-format tuples) ----
    fn enc_text_tuple(out: &mut Vec<u8>, vals: &[&str]) {
        out.extend_from_slice(&u16::try_from(vals.len()).expect("cols").to_be_bytes());
        for v in vals {
            out.push(b't');
            out.extend_from_slice(&u32::try_from(v.len()).expect("len").to_be_bytes());
            out.extend_from_slice(v.as_bytes());
        }
    }

    fn raw_insert(vals: &[&str]) -> Bytes {
        let mut o = vec![b'I'];
        o.extend_from_slice(&1u32.to_be_bytes());
        o.push(b'N');
        enc_text_tuple(&mut o, vals);
        Bytes::from(o)
    }

    fn raw_delete(old: &[&str]) -> Bytes {
        let mut o = vec![b'D'];
        o.extend_from_slice(&1u32.to_be_bytes());
        o.push(b'K');
        enc_text_tuple(&mut o, old);
        Bytes::from(o)
    }

    fn raw_update(old_key: &[&str], new: &[&str]) -> Bytes {
        let mut o = vec![b'U'];
        o.extend_from_slice(&1u32.to_be_bytes());
        o.push(b'K');
        enc_text_tuple(&mut o, old_key);
        o.push(b'N');
        enc_text_tuple(&mut o, new);
        Bytes::from(o)
    }

    fn raw_truncate() -> Bytes {
        let mut o = vec![b'T'];
        o.extend_from_slice(&1u32.to_be_bytes()); // nrel
        o.push(0); // flags
        o.extend_from_slice(&1u32.to_be_bytes()); // relation id
        Bytes::from(o)
    }

    #[test]
    fn raw_path_matches_eager_insert_pk_update_delete() {
        let rel = relation();
        let sch = schema();

        // insert(id=1) ; primary-key-changing update(1 -> 2) ; delete(id=2)
        let raw = vec![
            raw_insert(&["1", "a"]),
            raw_update(&["1", "a"], &["2", "b"]),
            raw_delete(&["2", "b"]),
        ];

        // Eager reference: build the DecodedChanges directly (bypassing the raw
        // bytes) with the same transform the pump used to run inline.
        let mut eager: Vec<DecodedChange> = Vec::new();
        eager.push(DecodedChange {
            op: ChangeOp::Create,
            row: tuple(&["1", "a"]),
        });
        push_update_change(
            &mut eager,
            &rel,
            Some(tuple(&["1", "a"])),
            tuple(&["2", "b"]),
        );
        eager.push(DecodedChange {
            op: ChangeOp::Delete,
            row: tuple(&["2", "b"]),
        });

        let raw_changes = decode_raw_changes(&rel, &raw).expect("raw decode");
        // insert + (delete-old-key + upsert-new) + delete
        assert_eq!(
            raw_changes.len(),
            4,
            "PK-changing update must expand to 2 rows"
        );

        let eager_batch = build_change_batch(&sch, &rel, &eager).expect("eager build");
        let raw_batch = build_change_batch(&sch, &rel, &raw_changes).expect("raw build");
        assert_eq!(
            eager_batch.record, raw_batch.record,
            "raw-buffered path must produce an identical ChangeBatch to the eager path"
        );
    }

    #[test]
    fn raw_path_matches_eager_update_no_key_change_and_truncate() {
        let rel = relation();
        let sch = schema();

        // non-key update(id=1, v a->b) ; truncate
        let raw = vec![raw_update(&["1", "a"], &["1", "b"]), raw_truncate()];

        let mut eager: Vec<DecodedChange> = Vec::new();
        push_update_change(
            &mut eager,
            &rel,
            Some(tuple(&["1", "a"])),
            tuple(&["1", "b"]),
        );
        eager.push(DecodedChange {
            op: ChangeOp::Truncate,
            row: TupleData { columns: vec![] },
        });

        let raw_changes = decode_raw_changes(&rel, &raw).expect("raw decode");
        // A non-PK update is a single upsert row (no delete-of-old-key).
        assert_eq!(
            raw_changes.len(),
            2,
            "non-key update stays one row (+ truncate)"
        );

        let eager_batch = build_change_batch(&sch, &rel, &eager).expect("eager build");
        let raw_batch = build_change_batch(&sch, &rel, &raw_changes).expect("raw build");
        assert_eq!(eager_batch.record, raw_batch.record);
    }

    #[test]
    fn coalesced_raw_chunks_build_in_source_order() {
        // Both sides take their schema and relation from the same generation, as
        // consecutive commits for one table do on the pump.
        let (sch, rel) = (schema(), relation());
        let mut first = PgChangeRows::new(
            Arc::clone(&sch),
            Arc::clone(&rel),
            vec![raw_insert(&["1", "a"])],
            Some(100),
        );
        let second = PgChangeRows::new(sch, rel, vec![raw_insert(&["2", "b"])], Some(200));
        assert!(
            first.try_append(second).is_none(),
            "compatible relation should append"
        );

        assert_eq!(first.num_rows_hint(), 2);
        assert_eq!(first.source_commit_ts_ms(), Some(200));
        let batch = Box::new(first).build().expect("build coalesced chunks");
        assert_eq!(batch.record.num_rows(), 2);
        let data = batch
            .record
            .column_by_name("data")
            .expect("data column")
            .as_struct();
        let ids = data
            .column_by_name("id")
            .expect("id column")
            .as_string::<i32>();
        assert_eq!(ids.value(0), "1");
        assert_eq!(ids.value(1), "2");
        assert_eq!(batch.source_commit_ts_ms(), Some(200));
    }

    #[test]
    fn raw_chunks_from_different_relation_generations_do_not_merge() {
        let sch = schema();
        let mut first = PgChangeRows::new(
            Arc::clone(&sch),
            relation(),
            vec![raw_insert(&["1", "a"])],
            Some(100),
        );
        let mut changed_relation = relation();
        Arc::make_mut(&mut changed_relation).columns[1].type_oid = 1_043;
        let second = PgChangeRows::new(
            sch,
            changed_relation,
            vec![raw_insert(&["2", "b"])],
            Some(200),
        );

        let returned = first.try_append(second);
        assert!(
            returned.is_some(),
            "different relation metadata must seal the current envelope"
        );
        assert_eq!(first.num_rows_hint(), 1);
        assert_eq!(first.source_commit_ts_ms(), Some(100));
    }

    #[test]
    fn structurally_identical_but_distinct_generations_decline_the_merge() {
        // Compatibility is decided by pointer, so a relation rebuilt from
        // scratch declines the merge even though it compares equal field for
        // field. That is the safe direction: a declined merge only costs one
        // extra envelope, while merging across a generation the decoder has
        // replaced could type the raw tuple bytes wrongly.
        let sch = schema();
        let mut first = PgChangeRows::new(
            Arc::clone(&sch),
            relation(),
            vec![raw_insert(&["1", "a"])],
            Some(100),
        );
        let second = PgChangeRows::new(sch, relation(), vec![raw_insert(&["2", "b"])], Some(200));

        assert!(
            first.try_append(second).is_some(),
            "a separately-allocated relation must not merge"
        );
        assert_eq!(first.num_rows_hint(), 1);

        // Same for the working schema: an adopted widening installs a new `Arc`.
        let rel = relation();
        let mut first = PgChangeRows::new(
            schema(),
            Arc::clone(&rel),
            vec![raw_insert(&["1", "a"])],
            Some(100),
        );
        let second = PgChangeRows::new(schema(), rel, vec![raw_insert(&["2", "b"])], Some(200));
        assert!(
            first.try_append(second).is_some(),
            "a separately-allocated schema must not merge"
        );
        assert_eq!(first.num_rows_hint(), 1);
    }

    #[test]
    fn pgchangerows_metadata_is_answered_without_decoding() {
        // is_empty is exact; num_rows_hint is an upper bound (+1 per UPDATE).
        let empty = PgChangeRows::new(schema(), relation(), vec![], Some(7));
        assert!(empty.is_empty());
        assert_eq!(empty.num_rows_hint(), 0);

        let rows = PgChangeRows::new(
            schema(),
            relation(),
            vec![
                raw_insert(&["1", "a"]),
                raw_update(&["1", "a"], &["2", "b"]),
            ],
            Some(7),
        );
        assert!(!rows.is_empty());
        // 2 messages + 1 (the UPDATE may split) = 3 upper bound; actual after
        // build is 3 (insert + delete-old + upsert-new).
        assert_eq!(rows.num_rows_hint(), 3);
        assert_eq!(rows.source_commit_ts_ms(), Some(7));
        assert!(!rows.is_heartbeat());

        let batch = Box::new(rows).build().expect("build");
        assert_eq!(batch.record.num_rows(), 3);
    }
}
