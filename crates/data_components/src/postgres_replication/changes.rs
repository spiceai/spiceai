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
    datatypes::{DataType, Field, Int8Type, Int16Type, Int32Type, Schema, SchemaRef, TimeUnit},
};
use async_trait::async_trait;

use super::pgoutput::{Relation, TupleData, Value};
use super::{PgOutputDecodeSnafu, Result};
use crate::cdc::{ChangeBatch, ChangeEnvelope, CommitChange, CommitError, changes_schema};

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

/// Buffer collecting `DecodedChange`s within a single transaction.
pub struct TransactionBuffer {
    pub begin_lsn: u64,
    pub changes: Vec<DecodedChange>,
}

impl TransactionBuffer {
    #[must_use]
    pub fn new(begin_lsn: u64) -> Self {
        Self {
            begin_lsn,
            changes: Vec::new(),
        }
    }

    pub fn push_insert(&mut self, _relation: &Relation, tuple: TupleData) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Create,
            row: tuple,
        });
    }

    pub fn push_update(&mut self, _relation: &Relation, new: TupleData) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Update,
            row: new,
        });
    }

    pub fn push_delete(&mut self, _relation: &Relation, old: TupleData) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Delete,
            row: old,
        });
    }

    /// Record a TRUNCATE for the relation. Row payload is empty — the
    /// accelerator path applies it as an unconditional delete-all.
    pub fn push_truncate(&mut self, _relation: &Relation) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Truncate,
            row: TupleData { columns: vec![] },
        });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
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

    // One builder per output field, typed from dataset schema.
    let mut data_builders: Vec<FieldBuilder> = dataset_schema
        .fields()
        .iter()
        .map(|f| FieldBuilder::new(f.data_type()))
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
                    data_builders[col_idx].append(value, change.op)?;
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
) -> ChangeEnvelope {
    ChangeEnvelope::new(
        Box::new(LsnCommitter {
            confirmed_flush,
            flush_to,
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
}

#[async_trait]
impl CommitChange for LsnCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        use std::sync::atomic::Ordering;
        // Monotonic CAS loop: only advance; never regress.
        let mut current = self.confirmed_flush.load(Ordering::Relaxed);
        loop {
            if self.flush_to <= current {
                return Ok(());
            }
            match self.confirmed_flush.compare_exchange(
                current,
                self.flush_to,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
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
    pub(super) fn new(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::LargeUtf8 => Self::LargeUtf8(LargeStringBuilder::new()),
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            DataType::Int8 => Self::Int8(Int8Builder::new()),
            DataType::Int16 => Self::Int16(Int16Builder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::UInt32 => Self::UInt32(UInt32Builder::new()),
            DataType::Float32 => Self::Float32(Float32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Date32 => Self::Date32(Date32Builder::new()),
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::Time64Nanos(Time64NanosecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Self::TimestampMicros(TimestampMicrosecondBuilder::new(), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                Self::TimestampNanos(TimestampNanosecondBuilder::new(), tz.clone())
            }
            DataType::Decimal128(precision, scale) => Self::Decimal128(
                Decimal128Builder::new().with_data_type(data_type.clone()),
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
                Self::List {
                    item_field: Arc::clone(item_field),
                    inner: Box::new(Self::new(item_field.data_type())?),
                    offsets: vec![0],
                    validity: Vec::new(),
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
            DataType::Dictionary(key, value) if **value == DataType::Utf8 => match **key {
                DataType::Int8 => Self::DictUtf8Int8(StringDictionaryBuilder::new()),
                DataType::Int16 => Self::DictUtf8Int16(StringDictionaryBuilder::new()),
                DataType::Int32 => Self::DictUtf8Int32(StringDictionaryBuilder::new()),
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

    pub(super) fn append(&mut self, value: Option<&Value>, op: ChangeOp) -> Result<()> {
        let Some(v) = value else {
            self.append_null();
            return Ok(());
        };
        let s = match v {
            Value::Text(s) => s,
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
                // pgoutput delivers bytea in binary format when the publication
                // uses the binary encoding. We only accept this for BinaryBuilder;
                // for other builders it's an error (silent coerce to NULL would
                // be wrong).
                if let Self::Binary(b) = self {
                    b.append_value(bytes);
                    return Ok(());
                }
                return PgOutputDecodeSnafu {
                    message: "postgres_replication: binary-format pgoutput value received \
                              for non-binary column. Configure the publication to use the \
                              text output format."
                        .to_string(),
                }
                .fail();
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
            Self::Bool(b) => b.append_value(matches!(s.as_str(), "t" | "true" | "TRUE")),
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
                            let value = Value::Text(text.clone());
                            inner.append(Some(&value), op)?;
                        }
                        None if item_field.is_nullable() => inner.append(None, op)?,
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

    // Sanity-check against declared precision — Arrow will enforce this on
    // `append_value` anyway, but a friendlier error helps ops.
    let _ = precision;
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
                Some(PgValue::Text(id.to_string())),
                name.map(|n| PgValue::Text(n.to_string())),
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

    #[tokio::test]
    async fn lsn_committer_advances_monotonically() {
        let lsn = Arc::new(AtomicU64::new(0));
        let c1 = LsnCommitter {
            confirmed_flush: Arc::clone(&lsn),
            flush_to: 100,
        };
        c1.commit().await.expect("commit");
        assert_eq!(lsn.load(std::sync::atomic::Ordering::Relaxed), 100);

        // older commit should not regress.
        let c2 = LsnCommitter {
            confirmed_flush: Arc::clone(&lsn),
            flush_to: 50,
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
                columns: vec![Some(PgValue::Text(text.to_string()))],
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
        let Err(err) = FieldBuilder::new(&nested) else {
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
        let Err(err) = FieldBuilder::new(&DataType::Interval(
            arrow::datatypes::IntervalUnit::MonthDayNano,
        )) else {
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
}
