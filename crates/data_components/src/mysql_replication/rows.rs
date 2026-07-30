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

//! Convert `MySQL` row values (binlog row images and snapshot rows) into the
//! canonical CDC [`ChangeBatch`] shape.
//!
//! Both sources deliver [`mysql_async::Value`]s: the binlog decoder produces
//! them from row images (via [`normalize_binlog_value`], which resolves
//! source-encoding quirks like ENUM indexes and SET bitmasks against the
//! table layout), and the snapshot path gets them straight from the binary
//! protocol. The [`FieldBuilder`]s here parse those values into the Arrow
//! types that the `MySQL` federated read provider declares for the same
//! columns, so CDC batches line up with the accelerator schema.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    LargeBinaryBuilder, LargeStringBuilder, ListArray, RecordBatch, StringArray, StringBuilder,
    StringDictionaryBuilder, StructArray, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{
    DataType, Field, Schema, SchemaRef, TimeUnit, UInt8Type, UInt16Type, UInt32Type,
};
use arrow_buffer::OffsetBuffer;
use arrow_buffer::i256;
use mysql_async::Value;
use mysql_async::binlog::value::BinlogValue;

use super::setup::SourceColumn;
use super::{DecodeSnafu, Error, Result};
use crate::cdc::{ChangeBatch, changes_schema};

/// One logical change decoded from the binlog (or synthesized by the
/// snapshot/truncate paths).
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedChange {
    pub op: ChangeOp,
    /// Full row image in source column order ([`super::setup::TableLayout`]
    /// order). Empty for [`ChangeOp::Truncate`].
    pub row: Vec<Value>,
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

/// Buffer collecting changes within a single source transaction.
pub struct TransactionBuffer {
    pub changes: Vec<DecodedChange>,
}

impl TransactionBuffer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            changes: Vec::new(),
        }
    }

    pub fn push_insert(&mut self, row: Vec<Value>) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Create,
            row,
        });
    }

    /// Buffer a binlog UPDATE.
    ///
    /// Accelerators apply [`ChangeOp::Update`] as an upsert keyed by the new
    /// primary key, so a primary-key change must also emit a delete for the
    /// old key; otherwise the old accelerated row is orphaned.
    pub fn push_update(&mut self, pk_source_indexes: &[usize], old: Vec<Value>, new: Vec<Value>) {
        let key_changed = pk_source_indexes
            .iter()
            .any(|idx| old.get(*idx) != new.get(*idx));
        if key_changed {
            self.changes.push(DecodedChange {
                op: ChangeOp::Delete,
                row: old,
            });
        }
        self.changes.push(DecodedChange {
            op: ChangeOp::Update,
            row: new,
        });
    }

    pub fn push_delete(&mut self, old: Vec<Value>) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Delete,
            row: old,
        });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// A synthesized TRUNCATE change. Row payload is empty — the accelerator
/// path applies it as an unconditional delete-all.
#[must_use]
pub fn truncate_change() -> DecodedChange {
    DecodedChange {
        op: ChangeOp::Truncate,
        row: Vec::new(),
    }
}

impl Default for TransactionBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve source-encoding quirks in a binlog row-image value against its
/// column definition, producing the same [`Value`] shape the snapshot path
/// delivers:
///
///   - `ENUM` arrives as the 1-based variant index → the variant label
///     (index 0 is `MySQL`'s invalid-value sentinel, `''`).
///   - `SET` arrives as a little-endian member bitmask → the joined member
///     list (`'a,b'`).
///   - JSON arrives as raw `JSONB` → its JSON text (done by the
///     `BinlogValue → Value` conversion itself).
///
/// Everything else passes through unchanged.
pub fn normalize_binlog_value(column: &SourceColumn, value: BinlogValue<'_>) -> Result<Value> {
    if let Some(variants) = &column.enum_variants
        && let BinlogValue::Value(Value::Int(index)) = &value
    {
        let index = *index;
        if index == 0 {
            // MySQL's empty-string sentinel for an invalid enum value.
            return Ok(Value::Bytes(Vec::new()));
        }
        let label = usize::try_from(index)
            .ok()
            .and_then(|i| variants.get(i - 1))
            .ok_or_else(|| Error::Decode {
                message: format!(
                    "ENUM index {index} out of range for column `{}` ({} variants). \
                     The source ENUM definition changed — restart the dataset to \
                     re-validate the schema.",
                    column.name,
                    variants.len()
                ),
            })?;
        return Ok(Value::Bytes(label.clone().into_bytes()));
    }

    if let Some(variants) = &column.set_variants
        && let BinlogValue::Value(Value::Bytes(mask_bytes)) = &value
    {
        if mask_bytes.len() > 8 {
            return DecodeSnafu {
                message: format!(
                    "SET bitmask for column `{}` is {} bytes (more than 64 members?)",
                    column.name,
                    mask_bytes.len()
                ),
            }
            .fail();
        }
        let mut mask: u64 = 0;
        for (i, b) in mask_bytes.iter().enumerate() {
            mask |= u64::from(*b) << (8 * i);
        }
        let mut members: Vec<&str> = Vec::new();
        for (i, variant) in variants.iter().enumerate() {
            if mask & (1 << i) != 0 {
                members.push(variant);
            }
        }
        return Ok(Value::Bytes(members.join(",").into_bytes()));
    }

    Value::try_from(value).map_err(|e| Error::Decode {
        message: format!(
            "column `{}`: {e}. Partial JSON row images cannot be applied — set \
             `binlog_row_value_options = ''` on the source server.",
            column.name
        ),
    })
}

/// Build a [`ChangeBatch`] from decoded changes, typing the `data` struct to
/// the accelerator's Arrow schema.
///
/// `column_map[i]` is the source row-image index for dataset field `i` (from
/// [`super::setup::TableLayout::column_map`]). `primary_keys` are the
/// dataset's declared PK column names, repeated into the batch's
/// `primary_keys` list column for every row.
///
/// **Nullability:** the `data` struct is built with every field nullable
/// regardless of the dataset schema's declared nullability, matching the
/// other CDC sources (see `postgres_replication::changes::build_change_batch`
/// for the full rationale) — zero-date coercion and TRUNCATE rows produce
/// nulls in otherwise non-null columns, and downstream casts re-tighten.
pub fn build_change_batch(
    dataset_schema: &SchemaRef,
    primary_keys: &[String],
    column_map: &[usize],
    changes: &[DecodedChange],
) -> Result<ChangeBatch> {
    let num_rows = changes.len();
    let nullable_schema = nullable_clone(dataset_schema);
    let wrapper_schema = changes_schema(&nullable_schema);

    let mut op_builder = StringBuilder::with_capacity(num_rows, num_rows * 2);
    let mut pk_offsets = Vec::<i32>::with_capacity(num_rows + 1);
    pk_offsets.push(0);
    let mut pk_values: Vec<&str> = Vec::with_capacity(num_rows.saturating_mul(primary_keys.len()));

    let mut data_builders: Vec<FieldBuilder> = dataset_schema
        .fields()
        .iter()
        .map(|f| FieldBuilder::new(f.data_type()))
        .collect::<Result<Vec<_>>>()?;

    for change in changes {
        op_builder.append_value(change.op.as_str());
        pk_values.extend(primary_keys.iter().map(String::as_str));
        pk_offsets.push(i32::try_from(pk_values.len()).map_err(|e| Error::Decode {
            message: format!("too many primary keys: {e}"),
        })?);

        for (field_idx, source_idx) in column_map.iter().enumerate() {
            if change.op == ChangeOp::Truncate {
                data_builders[field_idx].append_null();
                continue;
            }
            let value = change.row.get(*source_idx).ok_or_else(|| Error::Decode {
                message: format!(
                    "row image has {} columns but dataset field #{field_idx} maps to source \
                     column #{source_idx} — the source table layout changed mid-stream. \
                     Restart the dataset to re-validate the schema.",
                    change.row.len()
                ),
            })?;
            data_builders[field_idx].append(value)?;
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
    .map_err(|e| Error::SchemaMismatch {
        message: format!("failed to build change record batch: {e}"),
    })?;

    ChangeBatch::try_new(record).map_err(|e| Error::SchemaMismatch {
        message: format!("change batch validation failed: {e}"),
    })
}

/// Return a clone of `schema` where every field is marked nullable. See the
/// note on [`build_change_batch`].
pub(super) fn nullable_clone(schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Per-field Arrow builder that accepts [`mysql_async::Value`]s and parses
/// them into the typed column.
///
/// Type coverage matches what `datafusion-table-providers`' `MySQL` provider
/// exposes via `read_provider()` — the dataset's Arrow schema flows through
/// that path, so mismatches here would fail `StructArray` validation.
pub(super) enum FieldBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Binary(BinaryBuilder),
    LargeBinary(LargeBinaryBuilder),
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    /// Also the target for `BIT(n)` columns, whose values arrive as raw
    /// big-endian bytes.
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    Time64Nanos(Time64NanosecondBuilder),
    TimestampMicros(TimestampMicrosecondBuilder),
    TimestampNanos(TimestampNanosecondBuilder),
    /// `Decimal128(precision, scale)`
    Decimal128(Decimal128Builder, u8, i8),
    /// `Decimal256(precision, scale)` — DECIMAL columns with precision > 38.
    Decimal256(Decimal256Builder, u8, i8),
    /// The `MySQL` provider maps ENUM columns to `Dictionary(UInt16, Utf8)`.
    /// Values arrive as the variant label (resolved from the binlog index by
    /// [`normalize_binlog_value`]).
    DictUtf8UInt8(StringDictionaryBuilder<UInt8Type>),
    DictUtf8UInt16(StringDictionaryBuilder<UInt16Type>),
    DictUtf8UInt32(StringDictionaryBuilder<UInt32Type>),
}

impl FieldBuilder {
    pub(super) fn new(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::LargeUtf8 => Self::LargeUtf8(LargeStringBuilder::new()),
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            DataType::LargeBinary => Self::LargeBinary(LargeBinaryBuilder::new()),
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            DataType::Int8 => Self::Int8(Int8Builder::new()),
            DataType::Int16 => Self::Int16(Int16Builder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::UInt8 => Self::UInt8(UInt8Builder::new()),
            DataType::UInt16 => Self::UInt16(UInt16Builder::new()),
            DataType::UInt32 => Self::UInt32(UInt32Builder::new()),
            DataType::UInt64 => Self::UInt64(UInt64Builder::new()),
            DataType::Float32 => Self::Float32(Float32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Date32 => Self::Date32(Date32Builder::new()),
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::Time64Nanos(Time64NanosecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Self::TimestampMicros(
                TimestampMicrosecondBuilder::new()
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => Self::TimestampNanos(
                TimestampNanosecondBuilder::new()
                    .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())),
            ),
            DataType::Decimal128(precision, scale) => Self::Decimal128(
                Decimal128Builder::new().with_data_type(data_type.clone()),
                *precision,
                *scale,
            ),
            DataType::Decimal256(precision, scale) => Self::Decimal256(
                Decimal256Builder::new().with_data_type(data_type.clone()),
                *precision,
                *scale,
            ),
            DataType::Dictionary(key, value) if **value == DataType::Utf8 => match **key {
                DataType::UInt8 => Self::DictUtf8UInt8(StringDictionaryBuilder::new()),
                DataType::UInt16 => Self::DictUtf8UInt16(StringDictionaryBuilder::new()),
                DataType::UInt32 => Self::DictUtf8UInt32(StringDictionaryBuilder::new()),
                ref other => {
                    return DecodeSnafu {
                        message: format!(
                            "mysql_replication: unsupported dictionary key type {other} \
                             (only UInt8/UInt16/UInt32 keys with Utf8 values are supported)"
                        ),
                    }
                    .fail();
                }
            },
            other => {
                return DecodeSnafu {
                    message: format!(
                        "mysql_replication: unsupported Arrow data type in dataset schema: \
                         {other}. Exclude the column from the dataset schema or override its \
                         type via the dataset's `columns:` definition."
                    ),
                }
                .fail();
            }
        })
    }

    pub(super) fn append(&mut self, value: &Value) -> Result<()> {
        if matches!(value, Value::NULL) {
            self.append_null();
            return Ok(());
        }
        match self {
            Self::Utf8(b) => b.append_value(value_to_string(value)?),
            Self::LargeUtf8(b) => b.append_value(value_to_string(value)?),
            Self::Binary(b) => b.append_value(value_to_bytes(value)?),
            Self::LargeBinary(b) => b.append_value(value_to_bytes(value)?),
            Self::Bool(b) => b.append_value(value_to_bool(value)?),
            Self::Int8(b) => b.append_value(value_to_signed(value, "Int8")?),
            Self::Int16(b) => b.append_value(value_to_signed(value, "Int16")?),
            Self::Int32(b) => b.append_value(value_to_signed(value, "Int32")?),
            Self::Int64(b) => b.append_value(value_to_signed(value, "Int64")?),
            Self::UInt8(b) => b.append_value(value_to_unsigned(value, "UInt8")?),
            Self::UInt16(b) => b.append_value(value_to_unsigned(value, "UInt16")?),
            Self::UInt32(b) => b.append_value(value_to_unsigned(value, "UInt32")?),
            Self::UInt64(b) => b.append_value(value_to_unsigned(value, "UInt64")?),
            Self::Float32(b) => {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "FLOAT source columns arrive at f32 precision already; wider values \
                              reaching a user-declared Float32 column accept the truncation"
                )]
                let v = value_to_f64(value)? as f32;
                b.append_value(v);
            }
            Self::Float64(b) => b.append_value(value_to_f64(value)?),
            Self::Date32(b) => match value_to_date32(value)? {
                Some(days) => b.append_value(days),
                // MySQL zero-date sentinel — match the read provider's
                // default `zero_date_behavior: null`.
                None => b.append_null(),
            },
            Self::Time64Nanos(b) => b.append_value(value_to_time_nanos(value)?),
            Self::TimestampMicros(b) => match value_to_timestamp_micros(value)? {
                Some(micros) => b.append_value(micros),
                None => b.append_null(),
            },
            Self::TimestampNanos(b) => match value_to_timestamp_micros(value)? {
                Some(micros) => {
                    b.append_value(micros.checked_mul(1000).ok_or_else(|| Error::Decode {
                        message: format!("timestamp {micros}µs overflows nanoseconds"),
                    })?);
                }
                None => b.append_null(),
            },
            Self::Decimal128(b, precision, scale) => {
                let s = value_to_string(value)?;
                b.append_value(parse_decimal_to_i128(&s, *precision, *scale)?);
            }
            Self::Decimal256(b, precision, scale) => {
                let s = value_to_string(value)?;
                b.append_value(parse_decimal_to_i256(&s, *precision, *scale)?);
            }
            Self::DictUtf8UInt8(b) => {
                append_dict(b.append(value_to_string(value)?))?;
            }
            Self::DictUtf8UInt16(b) => {
                append_dict(b.append(value_to_string(value)?))?;
            }
            Self::DictUtf8UInt32(b) => {
                append_dict(b.append(value_to_string(value)?))?;
            }
        }
        Ok(())
    }

    pub(super) fn append_null(&mut self) {
        match self {
            Self::Utf8(b) => b.append_null(),
            Self::LargeUtf8(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::LargeBinary(b) => b.append_null(),
            Self::Bool(b) => b.append_null(),
            Self::Int8(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt8(b) => b.append_null(),
            Self::UInt16(b) => b.append_null(),
            Self::UInt32(b) => b.append_null(),
            Self::UInt64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::Time64Nanos(b) => b.append_null(),
            Self::TimestampMicros(b) => b.append_null(),
            Self::TimestampNanos(b) => b.append_null(),
            Self::Decimal128(b, _, _) => b.append_null(),
            Self::Decimal256(b, _, _) => b.append_null(),
            Self::DictUtf8UInt8(b) => b.append_null(),
            Self::DictUtf8UInt16(b) => b.append_null(),
            Self::DictUtf8UInt32(b) => b.append_null(),
        }
    }

    pub(super) fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::LargeUtf8(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
            Self::LargeBinary(mut b) => Arc::new(b.finish()),
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::Int8(mut b) => Arc::new(b.finish()),
            Self::Int16(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::UInt8(mut b) => Arc::new(b.finish()),
            Self::UInt16(mut b) => Arc::new(b.finish()),
            Self::UInt32(mut b) => Arc::new(b.finish()),
            Self::UInt64(mut b) => Arc::new(b.finish()),
            Self::Float32(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::Time64Nanos(mut b) => Arc::new(b.finish()),
            Self::TimestampMicros(mut b) => Arc::new(b.finish()),
            Self::TimestampNanos(mut b) => Arc::new(b.finish()),
            Self::Decimal128(mut b, _, _) => Arc::new(b.finish()),
            Self::Decimal256(mut b, _, _) => Arc::new(b.finish()),
            Self::DictUtf8UInt8(mut b) => Arc::new(b.finish()),
            Self::DictUtf8UInt16(mut b) => Arc::new(b.finish()),
            Self::DictUtf8UInt32(mut b) => Arc::new(b.finish()),
        }
    }
}

fn append_dict<K>(result: std::result::Result<K, arrow::error::ArrowError>) -> Result<()> {
    result.map(|_| ()).map_err(|e| Error::Decode {
        message: format!("dictionary append: {e}"),
    })
}

/// Borrows string payloads straight out of `Value::Bytes` (the dominant case
/// — the Arrow builder copies once from the borrow); only the rare
/// numeric/temporal fallbacks for user-overridden text columns allocate.
fn value_to_string(value: &Value) -> Result<std::borrow::Cow<'_, str>> {
    use std::borrow::Cow;
    match value {
        Value::Bytes(bytes) => {
            std::str::from_utf8(bytes)
                .map(Cow::Borrowed)
                .map_err(|_| Error::Decode {
                    message: "non-UTF-8 bytes for a text column. Declare the column as a binary \
                          type (or exclude it from the dataset schema)."
                        .to_string(),
                })
        }
        Value::Int(v) => Ok(Cow::Owned(v.to_string())),
        Value::UInt(v) => Ok(Cow::Owned(v.to_string())),
        Value::Float(v) => Ok(Cow::Owned(v.to_string())),
        Value::Double(v) => Ok(Cow::Owned(v.to_string())),
        Value::Date(year, month, day, 0, 0, 0, 0) => {
            Ok(Cow::Owned(format!("{year:04}-{month:02}-{day:02}")))
        }
        Value::Date(year, month, day, hour, minute, second, micros) => Ok(Cow::Owned(format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
        ))),
        Value::Time(negative, days, hour, minute, second, micros) => {
            let sign = if *negative { "-" } else { "" };
            let hours = u32::from(*hour) + days * 24;
            Ok(Cow::Owned(format!(
                "{sign}{hours:02}:{minute:02}:{second:02}.{micros:06}"
            )))
        }
        Value::NULL => unreachable!("NULL handled by append()"),
    }
}

fn value_to_bytes(value: &Value) -> Result<&[u8]> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        other => DecodeSnafu {
            message: format!("expected raw bytes for a binary column, got {other:?}"),
        }
        .fail(),
    }
}

fn value_to_bool(value: &Value) -> Result<bool> {
    match value {
        Value::Int(v) => Ok(*v != 0),
        Value::UInt(v) => Ok(*v != 0),
        Value::Bytes(b) => match std::str::from_utf8(b).map(str::trim) {
            Ok("0" | "false" | "FALSE") => Ok(false),
            Ok("1" | "true" | "TRUE") => Ok(true),
            _ => DecodeSnafu {
                message: format!("cannot interpret {value:?} as boolean"),
            }
            .fail(),
        },
        other => DecodeSnafu {
            message: format!("cannot interpret {other:?} as boolean"),
        }
        .fail(),
    }
}

fn value_to_signed<T>(value: &Value, target: &str) -> Result<T>
where
    T: TryFrom<i64> + TryFrom<u64> + std::str::FromStr,
{
    let out_of_range = |v: &dyn std::fmt::Debug| Error::Decode {
        message: format!(
            "value {v:?} does not fit dataset column type {target}. UNSIGNED source columns \
             map to signed Arrow types on this connector; values above the signed maximum \
             cannot be represented — widen the dataset column via `columns:` or alter the \
             source column."
        ),
    };
    match value {
        Value::Int(v) => T::try_from(*v).map_err(|_| out_of_range(v)),
        Value::UInt(v) => T::try_from(*v).map_err(|_| out_of_range(v)),
        Value::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.trim().parse::<T>().ok())
            .ok_or_else(|| out_of_range(&String::from_utf8_lossy(b).into_owned())),
        other => DecodeSnafu {
            message: format!("cannot interpret {other:?} as {target}"),
        }
        .fail(),
    }
}

fn value_to_unsigned<T>(value: &Value, target: &str) -> Result<T>
where
    T: TryFrom<u64> + std::fmt::Debug,
{
    let out_of_range = |v: &dyn std::fmt::Debug| Error::Decode {
        message: format!("value {v:?} does not fit dataset column type {target}"),
    };
    match value {
        Value::UInt(v) => T::try_from(*v).map_err(|_| out_of_range(v)),
        Value::Int(v) => u64::try_from(*v)
            .ok()
            .and_then(|v| T::try_from(v).ok())
            .ok_or_else(|| out_of_range(v)),
        // BIT(n) values arrive as raw big-endian bytes (up to 8).
        Value::Bytes(bytes) => {
            if bytes.len() > 8 {
                return DecodeSnafu {
                    message: format!("BIT value of {} bytes does not fit u64", bytes.len()),
                }
                .fail();
            }
            let mut acc: u64 = 0;
            for b in bytes {
                acc = (acc << 8) | u64::from(*b);
            }
            T::try_from(acc).map_err(|_| out_of_range(&acc))
        }
        other => DecodeSnafu {
            message: format!("cannot interpret {other:?} as {target}"),
        }
        .fail(),
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "u64/i64 → f64 for FLOAT/DOUBLE dataset columns is inherently lossy for \
              >2^53 values, matching MySQL's own float semantics"
)]
fn value_to_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Float(v) => Ok(f64::from(*v)),
        Value::Double(v) => Ok(*v),
        Value::Int(v) => Ok(*v as f64),
        Value::UInt(v) => Ok(*v as f64),
        Value::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .ok_or_else(|| Error::Decode {
                message: format!(
                    "cannot parse {:?} as a float",
                    String::from_utf8_lossy(b).into_owned()
                ),
            }),
        other => DecodeSnafu {
            message: format!("cannot interpret {other:?} as a float"),
        }
        .fail(),
    }
}

/// Days since the Unix epoch, or `None` for `MySQL`'s zero-date sentinel.
fn value_to_date32(value: &Value) -> Result<Option<i32>> {
    match value {
        // 0000-00-00 (or another out-of-range sentinel) yields None from
        // `from_ymd_opt` and coerces to NULL.
        Value::Date(y, m, d, _, _, _, _) => {
            Ok(
                chrono::NaiveDate::from_ymd_opt(i32::from(*y), u32::from(*m), u32::from(*d))
                    .map(days_since_epoch),
            )
        }
        Value::Bytes(b) => {
            let s = std::str::from_utf8(b).map_err(|_| Error::Decode {
                message: "non-UTF-8 bytes for a DATE column".to_string(),
            })?;
            let date = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").map_err(|e| {
                Error::Decode {
                    message: format!("DATE parse '{s}': {e}"),
                }
            })?;
            Ok(Some(days_since_epoch(date)))
        }
        other => DecodeSnafu {
            message: format!("cannot interpret {other:?} as a DATE"),
        }
        .fail(),
    }
}

fn days_since_epoch(date: chrono::NaiveDate) -> i32 {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default();
    i32::try_from(date.signed_duration_since(epoch).num_days()).unwrap_or(i32::MAX)
}

fn value_to_time_nanos(value: &Value) -> Result<i64> {
    match value {
        Value::Time(neg, days, h, m, s, micro) => {
            if *neg {
                return DecodeSnafu {
                    message: "negative TIME values cannot be represented as Arrow Time64. \
                              Exclude the column or store the value as seconds."
                        .to_string(),
                }
                .fail();
            }
            let secs = i64::from(*days) * 86_400
                + i64::from(*h) * 3_600
                + i64::from(*m) * 60
                + i64::from(*s);
            Ok(secs * 1_000_000_000 + i64::from(*micro) * 1_000)
        }
        Value::Bytes(b) => {
            use chrono::Timelike;
            let s = std::str::from_utf8(b).map_err(|_| Error::Decode {
                message: "non-UTF-8 bytes for a TIME column".to_string(),
            })?;
            let t = chrono::NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f").map_err(|e| {
                Error::Decode {
                    message: format!("TIME parse '{s}': {e}"),
                }
            })?;
            Ok(
                i64::from(t.num_seconds_from_midnight()) * 1_000_000_000
                    + i64::from(t.nanosecond()),
            )
        }
        other => DecodeSnafu {
            message: format!("cannot interpret {other:?} as a TIME"),
        }
        .fail(),
    }
}

/// Microseconds since the Unix epoch, or `None` for the zero-datetime
/// sentinel.
///
/// Accepts the three shapes `MySQL` delivers:
///   - `Value::Date` — DATETIME from the binlog and both DATETIME/TIMESTAMP
///     from the snapshot's binary protocol (TIMESTAMP already rendered in
///     the session time zone, which the snapshot pins to UTC).
///   - `Value::Bytes` of unix seconds (`"1699999999"` / `"1699999999.123456"`)
///     — TIMESTAMP from the binlog, which stores UTC seconds.
///   - `Value::Bytes` of a formatted datetime — defensive fallback.
fn value_to_timestamp_micros(value: &Value) -> Result<Option<i64>> {
    match value {
        Value::Date(year, month, day, hour, minute, second, micros) => {
            let Some(date) = chrono::NaiveDate::from_ymd_opt(
                i32::from(*year),
                u32::from(*month),
                u32::from(*day),
            ) else {
                return Ok(None); // zero-datetime sentinel
            };
            let time = chrono::NaiveTime::from_hms_micro_opt(
                u32::from(*hour),
                u32::from(*minute),
                u32::from(*second),
                *micros,
            )
            .ok_or_else(|| Error::Decode {
                message: format!("invalid time component {hour}:{minute}:{second}.{micros}"),
            })?;
            Ok(Some(date.and_time(time).and_utc().timestamp_micros()))
        }
        Value::Bytes(b) => {
            let s = std::str::from_utf8(b)
                .map_err(|_| Error::Decode {
                    message: "non-UTF-8 bytes for a TIMESTAMP column".to_string(),
                })?
                .trim();
            if s.contains('-') {
                // Formatted datetime fallback.
                let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").map_err(
                    |e| Error::Decode {
                        message: format!("TIMESTAMP parse '{s}': {e}"),
                    },
                )?;
                return Ok(Some(dt.and_utc().timestamp_micros()));
            }
            // Unix seconds with optional fractional part (binlog TIMESTAMP2).
            let (secs_str, frac_str) = match s.split_once('.') {
                Some((sec, frac)) => (sec, frac),
                None => (s, ""),
            };
            let secs: i64 = secs_str.parse().map_err(|e| Error::Decode {
                message: format!("TIMESTAMP unix-seconds parse '{s}': {e}"),
            })?;
            if secs == 0 && frac_str.is_empty() {
                // Zero-timestamp sentinel ('0000-00-00 00:00:00' stores as 0).
                return Ok(None);
            }
            let mut micros_frac: i64 = 0;
            if !frac_str.is_empty() {
                let padded = format!("{frac_str:0<6}");
                micros_frac =
                    padded
                        .get(..6)
                        .unwrap_or("0")
                        .parse()
                        .map_err(|e| Error::Decode {
                            message: format!("TIMESTAMP fraction parse '{s}': {e}"),
                        })?;
            }
            let micros = secs
                .checked_mul(1_000_000)
                .and_then(|v| v.checked_add(micros_frac))
                .ok_or_else(|| Error::Decode {
                    message: format!("TIMESTAMP '{s}' overflows microseconds"),
                })?;
            Ok(Some(micros))
        }
        other => DecodeSnafu {
            message: format!("cannot interpret {other:?} as a TIMESTAMP"),
        }
        .fail(),
    }
}

/// Parse a plain decimal string (`-123.45`) into an i128 scaled to the Arrow
/// column's declared scale. Binlog NEWDECIMAL values carry exactly the
/// column's scale, which matches the schema derived from the same column.
fn parse_decimal_to_i128(s: &str, precision: u8, scale: i8) -> Result<i128> {
    let digits = decimal_digits_at_scale(s, precision, scale)?;
    digits.parse::<i128>().map_err(|e| Error::Decode {
        message: format!("decimal '{s}' parse to i128: {e}"),
    })
}

fn parse_decimal_to_i256(s: &str, precision: u8, scale: i8) -> Result<i256> {
    let digits = decimal_digits_at_scale(s, precision, scale)?;
    i256::from_string(&digits).ok_or_else(|| Error::Decode {
        message: format!("decimal '{s}' parse to i256 failed"),
    })
}

/// Rescale a decimal string to `scale` fractional digits and return the
/// combined signed digit string (`-123.4` at scale 2 → `-12340`).
fn decimal_digits_at_scale(s: &str, precision: u8, scale: i8) -> Result<String> {
    let trimmed = s.trim();
    let (sign, rest) = match trimmed.strip_prefix('-') {
        Some(r) => ("-", r),
        None => ("", trimmed.trim_start_matches('+')),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return DecodeSnafu {
            message: format!("empty decimal value '{s}'"),
        }
        .fail();
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return DecodeSnafu {
            message: format!("non-numeric decimal value '{s}'"),
        }
        .fail();
    }

    let target_scale = usize::try_from(scale.max(0)).unwrap_or(0);
    if frac_part.len() > target_scale {
        return DecodeSnafu {
            message: format!(
                "decimal value '{s}' has scale {} but dataset schema declares scale {scale}",
                frac_part.len()
            ),
        }
        .fail();
    }
    let mut frac = frac_part.to_string();
    while frac.len() < target_scale {
        frac.push('0');
    }

    let significant = int_part.trim_start_matches('0').len() + frac.len();
    if significant > usize::from(precision) {
        return DecodeSnafu {
            message: format!(
                "decimal value '{s}' exceeds declared precision {precision} (scale {scale})"
            ),
        }
        .fail();
    }

    Ok(format!("{sign}{int_part}{frac}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn source_column(name: &str) -> SourceColumn {
        SourceColumn {
            name: name.to_string(),
            column_type: "varchar(255)".to_string(),
            enum_variants: None,
            set_variants: None,
            is_primary_key: false,
        }
    }

    #[test]
    fn normalizes_enum_index_to_label() {
        let mut col = source_column("size");
        col.enum_variants = Some(Arc::from(vec!["small".to_string(), "medium".to_string()]));
        let normalized =
            normalize_binlog_value(&col, BinlogValue::Value(Value::Int(2))).expect("valid index");
        assert_eq!(normalized, Value::Bytes(b"medium".to_vec()));

        // Index 0 is MySQL's invalid-enum sentinel — the empty string.
        let sentinel =
            normalize_binlog_value(&col, BinlogValue::Value(Value::Int(0))).expect("sentinel");
        assert_eq!(sentinel, Value::Bytes(Vec::new()));

        let err = normalize_binlog_value(&col, BinlogValue::Value(Value::Int(3)))
            .expect_err("out of range index must error");
        assert!(err.to_string().contains("out of range"), "got: {err}");
    }

    #[test]
    fn normalizes_set_bitmask_to_member_list() {
        let mut col = source_column("flags");
        col.set_variants = Some(Arc::from(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]));
        // bit 0 (a) + bit 2 (c)
        let normalized =
            normalize_binlog_value(&col, BinlogValue::Value(Value::Bytes(vec![0b101])))
                .expect("valid mask");
        assert_eq!(normalized, Value::Bytes(b"a,c".to_vec()));

        let empty = normalize_binlog_value(&col, BinlogValue::Value(Value::Bytes(vec![0])))
            .expect("empty mask");
        assert_eq!(empty, Value::Bytes(Vec::new()));
    }

    #[test]
    fn signed_builder_rejects_overflow_with_actionable_message() {
        let mut b = FieldBuilder::new(&DataType::Int8).expect("builder");
        let err = b
            .append(&Value::UInt(200))
            .expect_err("200 does not fit i8");
        assert!(err.to_string().contains("UNSIGNED"), "got: {err}");
    }

    #[test]
    fn bit_bytes_fold_big_endian_into_u64() {
        let mut b = FieldBuilder::new(&DataType::UInt64).expect("builder");
        b.append(&Value::Bytes(vec![0x01, 0x00]))
            .expect("2-byte BIT");
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("u64 array");
        assert_eq!(arr.value(0), 256);
    }

    #[test]
    fn timestamp_accepts_binlog_unix_seconds_and_native_datetime() {
        let mut b =
            FieldBuilder::new(&DataType::Timestamp(TimeUnit::Microsecond, None)).expect("builder");
        // Binlog TIMESTAMP2 form: unix seconds string with fraction.
        b.append(&Value::Bytes(b"1700000000.250000".to_vec()))
            .expect("unix seconds");
        // Snapshot / DATETIME form.
        b.append(&Value::Date(2023, 11, 14, 22, 13, 20, 0))
            .expect("native datetime");
        // Zero-datetime sentinel coerces to NULL.
        b.append(&Value::Date(0, 0, 0, 0, 0, 0, 0))
            .expect("zero date");
        let arr = b.finish();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .expect("ts array");
        assert_eq!(arr.value(0), 1_700_000_000_250_000);
        assert_eq!(arr.value(1), 1_700_000_000_000_000);
        assert!(arr.is_null(2));
    }

    #[test]
    fn date_zero_sentinel_is_null() {
        let mut b = FieldBuilder::new(&DataType::Date32).expect("builder");
        b.append(&Value::Date(2024, 3, 1, 0, 0, 0, 0))
            .expect("date");
        b.append(&Value::Date(0, 0, 0, 0, 0, 0, 0))
            .expect("zero date");
        let arr = b.finish();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
    }

    #[test]
    fn decimal_scales_and_validates() {
        assert_eq!(
            parse_decimal_to_i128("-123.4", 10, 2).expect("parses"),
            -12340
        );
        assert_eq!(parse_decimal_to_i128("0.01", 10, 2).expect("parses"), 1);
        let err = parse_decimal_to_i128("1.234", 10, 2).expect_err("scale overflow");
        assert!(err.to_string().contains("scale"), "got: {err}");
    }

    #[test]
    fn decimal256_handles_wide_values() {
        // 40 significant digits — beyond i128's decimal38 ceiling.
        let wide = "9".repeat(40);
        let v = parse_decimal_to_i256(&wide, 40, 0).expect("parses");
        assert_eq!(v.to_string(), wide);
    }

    #[test]
    fn negative_time_errors() {
        let mut b = FieldBuilder::new(&DataType::Time64(TimeUnit::Nanosecond)).expect("builder");
        let err = b
            .append(&Value::Time(true, 0, 1, 0, 0, 0))
            .expect_err("negative TIME unsupported");
        assert!(err.to_string().contains("negative TIME"), "got: {err}");
    }

    #[test]
    fn pk_change_update_emits_delete_then_update() {
        let mut txn = TransactionBuffer::new();
        // PK is source column 0.
        txn.push_update(
            &[0],
            vec![Value::Int(1), Value::Bytes(b"old".to_vec())],
            vec![Value::Int(2), Value::Bytes(b"new".to_vec())],
        );
        assert_eq!(txn.changes.len(), 2);
        assert_eq!(txn.changes[0].op, ChangeOp::Delete);
        assert_eq!(txn.changes[0].row[0], Value::Int(1));
        assert_eq!(txn.changes[1].op, ChangeOp::Update);
        assert_eq!(txn.changes[1].row[0], Value::Int(2));
    }

    #[test]
    fn same_pk_update_emits_single_update() {
        let mut txn = TransactionBuffer::new();
        txn.push_update(
            &[0],
            vec![Value::Int(1), Value::Bytes(b"old".to_vec())],
            vec![Value::Int(1), Value::Bytes(b"new".to_vec())],
        );
        assert_eq!(txn.changes.len(), 1);
        assert_eq!(txn.changes[0].op, ChangeOp::Update);
    }

    #[test]
    fn builds_change_batch_with_column_remapping() {
        use arrow::datatypes::Field;
        // Dataset declares (name, id) while the source layout is (id, name).
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let column_map = vec![1, 0];
        let changes = vec![
            DecodedChange {
                op: ChangeOp::Create,
                row: vec![Value::Int(7), Value::Bytes(b"seven".to_vec())],
            },
            DecodedChange {
                op: ChangeOp::Delete,
                row: vec![Value::Int(8), Value::Bytes(b"eight".to_vec())],
            },
        ];
        let batch = build_change_batch(&schema, &["id".to_string()], &column_map, &changes)
            .expect("batch builds");
        assert_eq!(batch.record.num_rows(), 2);
        assert_eq!(batch.primary_keys(0), vec!["id".to_string()]);
        let data = batch.data_batch();
        let names = data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(names.value(0), "seven");
        let ids = data
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("i32");
        assert_eq!(ids.value(0), 7);
        assert_eq!(ids.value(1), 8);
    }

    #[test]
    fn truncate_rows_build_as_all_null() {
        use arrow::datatypes::Field;
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let changes = vec![DecodedChange {
            op: ChangeOp::Truncate,
            row: Vec::new(),
        }];
        let batch = build_change_batch(&schema, &["id".to_string()], &[0], &changes)
            .expect("truncate batch builds");
        assert_eq!(batch.record.num_rows(), 1);
        assert!(matches!(batch.op(0), crate::cdc::ChangeOperation::Truncate));
    }
}
