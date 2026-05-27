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

//! Local Arrow → JSON conversion for `RunQuery` payloads.
//!
//! Produces the wire shape the Spice Cloud portal's Query tab expects:
//!
//! ```text
//! {
//!   "columns": [{ "name": "...", "data_type": "Utf8" | "Int64" | ... }],
//!   "rows": [[v1, v2, ...], ...],
//!   "row_count": N,
//!   "truncated": bool
//! }
//! ```
//!
//! Type encoding rules:
//! - Numbers stay numbers (Int*/UInt*/Float* → JSON numbers; non-finite f64
//!   collapses to `null` because JSON has no representation for NaN/Inf).
//! - Strings stay strings.
//! - Booleans stay booleans.
//! - SQL NULL → JSON `null`.
//! - Lists / Structs / Maps become JSON arrays / objects recursively.
//! - Timestamps render as ISO 8601 strings (`YYYY-MM-DDTHH:MM:SS[.fff…]Z`
//!   when no timezone, RFC 3339 with offset when one is present).
//! - Dates render as `YYYY-MM-DD`, Times render as `HH:MM:SS[.fff…]`.
//! - Binary / FixedSizeBinary / LargeBinary / BinaryView render as base64
//!   (standard alphabet, padded).
//! - Decimals render as their canonical decimal string.
//! - Dictionary columns are transparent: encoded as the underlying value.
//! - Anything we cannot encode falls back to its `Debug` string so the
//!   portal sees *something* rather than a hard error.
//!
//! ## Size budget
//!
//! [`encode_record_batches`] caps the serialized payload at 5 MiB. If
//! adding a row would push us past the budget we stop appending and set
//! `truncated: true`. This protects the cloud control plane from being
//! handed an unbounded payload by a chatty query (e.g. `SELECT *` on a
//! large table).
//!
//! ## Row cap
//!
//! Callers should pre-cap the number of rows with their own `max_rows`;
//! [`encode_record_batches`] takes a `row_cap` for defense-in-depth and
//! still honors the byte budget independently.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, RecordBatch, StringArray,
    StringViewArray, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, TimeZone, Utc};
use serde_json::{Map, Number, Value};

/// Hard ceiling on serialized payload bytes. RunQuery responses larger
/// than this are truncated and `truncated: true` is set on the envelope.
pub const PAYLOAD_SIZE_BUDGET_BYTES: usize = 5 * 1024 * 1024;

/// Encode a list of `RecordBatch`es into the RunQuery envelope shape.
///
/// - `row_cap`: stop after this many rows, set `truncated: true`. A value
///   of `usize::MAX` means "row cap disabled".
/// - Honors [`PAYLOAD_SIZE_BUDGET_BYTES`] independently — if the encoded
///   JSON would exceed the budget we stop and mark `truncated: true`.
///
/// The schema is taken from the first batch in `batches`. If the query
/// produced zero batches (zero-row result), callers should use
/// [`encode_record_batches_with_schema`] so the envelope still reports
/// the correct column metadata.
#[must_use]
pub fn encode_record_batches(batches: &[RecordBatch], row_cap: usize) -> Value {
    encode_record_batches_with_schema(batches, None, row_cap)
}

/// Like [`encode_record_batches`] but accepts an explicit schema so the
/// envelope still reports the correct columns when `batches` is empty.
/// Pass `None` to fall back to the schema of the first batch (matching
/// the [`encode_record_batches`] behaviour).
#[must_use]
pub fn encode_record_batches_with_schema(
    batches: &[RecordBatch],
    schema: Option<&arrow::datatypes::Schema>,
    row_cap: usize,
) -> Value {
    let columns_json = schema
        .map(columns_metadata_from_schema)
        .unwrap_or_else(|| columns_metadata(batches));

    // Account for the bytes the envelope already costs before we add any
    // rows: the serialized `columns` array (load-bearing for queries with
    // many columns or long aliases) plus a small fixed allowance for the
    // surrounding `{}` / keys / `row_count` / `truncated`. We compare
    // *cumulative* size to the budget so a wide schema cannot push the
    // final payload past the advertised cap even when `running_bytes`
    // for just the rows stays under budget.
    let columns_bytes = serde_json::to_string(&Value::Array(columns_json.clone()))
        .map(|s| s.len())
        .unwrap_or(0);
    const ENVELOPE_OVERHEAD_BYTES: usize = 128;
    let mut running_bytes: usize = columns_bytes + ENVELOPE_OVERHEAD_BYTES;

    let mut rows: Vec<Value> = Vec::new();
    let mut truncated = false;

    'outer: for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            if rows.len() >= row_cap {
                truncated = true;
                break 'outer;
            }
            let row = batch_row_to_json(batch, row_idx);
            // Cheap proxy for serialized size — `to_string` is O(n) but
            // we only do it on the row we're about to push.
            let row_bytes = serde_json::to_string(&row).map(|s| s.len()).unwrap_or(0)
                // +1 for the comma separator between rows.
                + 1;
            if running_bytes + row_bytes > PAYLOAD_SIZE_BUDGET_BYTES {
                truncated = true;
                break 'outer;
            }
            running_bytes += row_bytes;
            rows.push(row);
        }
    }

    let row_count = rows.len();
    let mut envelope = Map::new();
    envelope.insert("columns".to_string(), Value::Array(columns_json));
    envelope.insert("rows".to_string(), Value::Array(rows));
    envelope.insert(
        "row_count".to_string(),
        Value::Number(Number::from(row_count as u64)),
    );
    envelope.insert("truncated".to_string(), Value::Bool(truncated));
    Value::Object(envelope)
}

/// Build the `columns` array from the first non-empty batch's schema.
fn columns_metadata(batches: &[RecordBatch]) -> Vec<Value> {
    let Some(batch) = batches.first() else {
        return Vec::new();
    };
    columns_metadata_from_schema(batch.schema().as_ref())
}

fn columns_metadata_from_schema(schema: &arrow::datatypes::Schema) -> Vec<Value> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let mut obj = Map::new();
            obj.insert("name".to_string(), Value::String(f.name().clone()));
            obj.insert(
                "data_type".to_string(),
                Value::String(arrow_data_type_label(f.data_type())),
            );
            Value::Object(obj)
        })
        .collect()
}

/// Convert a single row of a `RecordBatch` to a JSON array, one cell per
/// column.
fn batch_row_to_json(batch: &RecordBatch, row_idx: usize) -> Value {
    let mut cells = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx);
        cells.push(array_value_to_json(col, row_idx));
    }
    Value::Array(cells)
}

/// Convert one cell to JSON. Dispatches on the array's data type.
///
/// Unknown / unsupported types fall back to the Arrow `Debug` rendering
/// so the portal still sees a string. We never panic on encode.
fn array_value_to_json(array: &ArrayRef, idx: usize) -> Value {
    if !array.is_valid(idx) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => downcast_bool(array, idx),
        DataType::Int8 => downcast_int::<Int8Array, i8>(array, idx, Number::from),
        DataType::Int16 => downcast_int::<Int16Array, i16>(array, idx, Number::from),
        DataType::Int32 => downcast_int::<Int32Array, i32>(array, idx, Number::from),
        DataType::Int64 => downcast_int::<Int64Array, i64>(array, idx, Number::from),
        DataType::UInt8 => downcast_int::<UInt8Array, u8>(array, idx, Number::from),
        DataType::UInt16 => downcast_int::<UInt16Array, u16>(array, idx, Number::from),
        DataType::UInt32 => downcast_int::<UInt32Array, u32>(array, idx, Number::from),
        DataType::UInt64 => downcast_int::<UInt64Array, u64>(array, idx, Number::from),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| f32_to_json(a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| f64_to_json(a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| Value::String(a.value(idx).to_string()))
            .unwrap_or(Value::Null),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| Value::String(a.value(idx).to_string()))
            .unwrap_or(Value::Null),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|a| Value::String(a.value(idx).to_string()))
            .unwrap_or(Value::Null),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|a| Value::String(BASE64_STANDARD.encode(a.value(idx))))
            .unwrap_or(Value::Null),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|a| Value::String(BASE64_STANDARD.encode(a.value(idx))))
            .unwrap_or(Value::Null),
        DataType::BinaryView => array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .map(|a| Value::String(BASE64_STANDARD.encode(a.value(idx))))
            .unwrap_or(Value::Null),
        DataType::FixedSizeBinary(_) => array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map(|a| Value::String(BASE64_STANDARD.encode(a.value(idx))))
            .unwrap_or(Value::Null),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| date32_to_json(a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| date64_to_json(a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::Time32(unit) => time32_to_json(array, idx, *unit),
        DataType::Time64(unit) => time64_to_json(array, idx, *unit),
        DataType::Timestamp(unit, tz) => timestamp_to_json(array, idx, *unit, tz.as_deref()),
        DataType::Decimal128(_, _) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map(|a| Value::String(a.value_as_string(idx)))
            .unwrap_or(Value::Null),
        DataType::Decimal256(_, _) => array
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .map(|a| Value::String(a.value_as_string(idx)))
            .unwrap_or(Value::Null),
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|a| nested_list_to_json(&a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::LargeList(_) => array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .map(|a| nested_list_to_json(&a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::FixedSizeList(_, _) => array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map(|a| nested_list_to_json(&a.value(idx)))
            .unwrap_or(Value::Null),
        DataType::Struct(fields) => array
            .as_any()
            .downcast_ref::<StructArray>()
            .map(|a| struct_to_json(a, idx, fields))
            .unwrap_or(Value::Null),
        DataType::Map(_, _) => array
            .as_any()
            .downcast_ref::<MapArray>()
            .map(|a| map_to_json(a, idx))
            .unwrap_or(Value::Null),
        DataType::Dictionary(_, _) => dictionary_to_json(array, idx),
        // Anything else (Union, RunEndEncoded, Interval, Duration, …)
        // falls back to a string rendering. We don't return Err — the
        // portal would rather see "8 hours" than a hard 500.
        _ => Value::String(format!("{:?}", array.slice(idx, 1))),
    }
}

fn downcast_bool(array: &ArrayRef, idx: usize) -> Value {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(|a| Value::Bool(a.value(idx)))
        .unwrap_or(Value::Null)
}

fn downcast_int<A, V>(array: &ArrayRef, idx: usize, f: impl Fn(V) -> Number) -> Value
where
    A: Array + ArrayValueAt<V> + 'static,
    V: Copy,
{
    array
        .as_any()
        .downcast_ref::<A>()
        .map(|a| Value::Number(f(a.value_at(idx))))
        .unwrap_or(Value::Null)
}

/// Tiny shim so the generic int downcaster can reach `value(idx)`.
trait ArrayValueAt<V> {
    fn value_at(&self, idx: usize) -> V;
}

macro_rules! impl_array_value_at {
    ($arr:ty, $val:ty) => {
        impl ArrayValueAt<$val> for $arr {
            fn value_at(&self, idx: usize) -> $val {
                self.value(idx)
            }
        }
    };
}
impl_array_value_at!(Int8Array, i8);
impl_array_value_at!(Int16Array, i16);
impl_array_value_at!(Int32Array, i32);
impl_array_value_at!(Int64Array, i64);
impl_array_value_at!(UInt8Array, u8);
impl_array_value_at!(UInt16Array, u16);
impl_array_value_at!(UInt32Array, u32);
impl_array_value_at!(UInt64Array, u64);

fn f32_to_json(v: f32) -> Value {
    if v.is_finite() {
        Number::from_f64(f64::from(v)).map(Value::Number).unwrap_or(Value::Null)
    } else {
        // JSON cannot represent NaN / +Inf / -Inf.
        Value::Null
    }
}

fn f64_to_json(v: f64) -> Value {
    if v.is_finite() {
        Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
    } else {
        Value::Null
    }
}

fn date32_to_json(days_since_epoch: i32) -> Value {
    let Some(epoch) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
        return Value::Null;
    };
    let Some(date) = epoch.checked_add_signed(chrono::Duration::days(i64::from(days_since_epoch)))
    else {
        return Value::Null;
    };
    Value::String(date.format("%Y-%m-%d").to_string())
}

fn date64_to_json(millis: i64) -> Value {
    let Some(naive) = DateTime::<Utc>::from_timestamp_millis(millis) else {
        return Value::Null;
    };
    Value::String(naive.naive_utc().date().format("%Y-%m-%d").to_string())
}

fn time32_to_json(array: &ArrayRef, idx: usize, unit: TimeUnit) -> Value {
    let value = match unit {
        TimeUnit::Second => array
            .as_any()
            .downcast_ref::<Time32SecondArray>()
            .map(|a| (i64::from(a.value(idx)), 1_000_000_000_i64)),
        TimeUnit::Millisecond => array
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .map(|a| (i64::from(a.value(idx)), 1_000_000_i64)),
        TimeUnit::Microsecond | TimeUnit::Nanosecond => None,
    };
    let Some((value, ns_per_unit)) = value else {
        return Value::Null;
    };
    naive_time_from_nanos(value.saturating_mul(ns_per_unit))
}

fn time64_to_json(array: &ArrayRef, idx: usize, unit: TimeUnit) -> Value {
    let value = match unit {
        TimeUnit::Microsecond => array
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .map(|a| a.value(idx).saturating_mul(1_000)),
        TimeUnit::Nanosecond => array
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .map(|a| a.value(idx)),
        TimeUnit::Second | TimeUnit::Millisecond => None,
    };
    let Some(nanos) = value else {
        return Value::Null;
    };
    naive_time_from_nanos(nanos)
}

fn naive_time_from_nanos(nanos: i64) -> Value {
    let secs = nanos.div_euclid(1_000_000_000) as u32;
    let frac = nanos.rem_euclid(1_000_000_000) as u32;
    let Some(time) = NaiveTime::from_num_seconds_from_midnight_opt(secs, frac) else {
        return Value::Null;
    };
    // Render with fractional seconds only when non-zero, to keep simple
    // values (00:00:00) tidy.
    let formatted = if frac == 0 {
        time.format("%H:%M:%S").to_string()
    } else {
        time.format("%H:%M:%S%.f").to_string()
    };
    Value::String(formatted)
}

fn timestamp_to_json(array: &ArrayRef, idx: usize, unit: TimeUnit, tz: Option<&str>) -> Value {
    let nanos: Option<i64> = match unit {
        TimeUnit::Second => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|a| a.value(idx).saturating_mul(1_000_000_000)),
        TimeUnit::Millisecond => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|a| a.value(idx).saturating_mul(1_000_000)),
        TimeUnit::Microsecond => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|a| a.value(idx).saturating_mul(1_000)),
        TimeUnit::Nanosecond => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| a.value(idx)),
    };
    let Some(nanos) = nanos else {
        return Value::Null;
    };

    let secs = nanos.div_euclid(1_000_000_000);
    let frac = (nanos.rem_euclid(1_000_000_000)) as u32;
    let Some(naive_dt) = DateTime::<Utc>::from_timestamp(secs, frac).map(|dt| dt.naive_utc())
    else {
        return Value::Null;
    };

    if let Some(tz) = tz {
        // Parse named offsets like "+07:00" / "-05:00" / "UTC" / "Z".
        if let Some(offset) = parse_fixed_offset(tz) {
            let dt = offset
                .from_utc_datetime(&naive_dt);
            return Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true));
        }
        // Otherwise stamp it UTC and append a note in the timezone slot.
        let dt = Utc.from_utc_datetime(&naive_dt);
        return Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true));
    }

    // No timezone — render as ISO 8601 with trailing Z to communicate
    // that the underlying timestamp is UTC-relative (Arrow timestamps
    // without a timezone are by convention treated as wall-clock; we
    // still pin them to a UTC interpretation for round-tripping).
    let dt = Utc.from_utc_datetime(&naive_dt);
    Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}

fn parse_fixed_offset(tz: &str) -> Option<FixedOffset> {
    if tz == "UTC" || tz == "Z" || tz == "+00:00" {
        return FixedOffset::east_opt(0);
    }
    // Parse "+HH:MM" or "-HH:MM".
    let (sign, rest) = match tz.chars().next() {
        Some('+') => (1, &tz[1..]),
        Some('-') => (-1, &tz[1..]),
        _ => return None,
    };
    let (hh, mm) = rest.split_once(':')?;
    let hours: i32 = hh.parse().ok()?;
    let mins: i32 = mm.parse().ok()?;
    let secs = sign * (hours * 3600 + mins * 60);
    FixedOffset::east_opt(secs)
}

fn nested_list_to_json(values: &ArrayRef) -> Value {
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        out.push(array_value_to_json(values, i));
    }
    Value::Array(out)
}

fn struct_to_json(array: &StructArray, idx: usize, fields: &[Arc<Field>]) -> Value {
    let mut obj = Map::new();
    for (col_idx, field) in fields.iter().enumerate() {
        let child = array.column(col_idx);
        obj.insert(field.name().clone(), array_value_to_json(child, idx));
    }
    Value::Object(obj)
}

fn map_to_json(array: &MapArray, idx: usize) -> Value {
    // MapArray laid out as a List of Struct<key, value>. We materialize
    // it as a JSON object when all keys are strings, otherwise as an
    // array of {key, value} pairs to avoid lossy coercion.
    let entries = array.value(idx);
    let Some(struct_array) = entries.as_any().downcast_ref::<StructArray>() else {
        return Value::Array(Vec::new());
    };

    let key_col = struct_array.column(0);
    let val_col = struct_array.column(1);

    let all_string_keys = matches!(
        key_col.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    );

    if all_string_keys {
        let mut obj = Map::new();
        for i in 0..struct_array.len() {
            let key = array_value_to_json(key_col, i);
            let val = array_value_to_json(val_col, i);
            if let Value::String(k) = key {
                obj.insert(k, val);
            } else {
                obj.insert(serde_json::to_string(&key).unwrap_or_default(), val);
            }
        }
        Value::Object(obj)
    } else {
        let mut pairs = Vec::with_capacity(struct_array.len());
        for i in 0..struct_array.len() {
            let mut pair = Map::new();
            pair.insert("key".to_string(), array_value_to_json(key_col, i));
            pair.insert("value".to_string(), array_value_to_json(val_col, i));
            pairs.push(Value::Object(pair));
        }
        Value::Array(pairs)
    }
}

fn dictionary_to_json(array: &ArrayRef, idx: usize) -> Value {
    // Dictionary columns: resolve to the underlying value at `idx`. We
    // use `arrow::compute::cast` to flatten only the row we need would
    // be expensive — instead we walk the dictionary directly.
    use arrow::array::AsArray as _;
    let dict = array.as_any_dictionary();
    let values = dict.values();
    let keys = dict.normalized_keys();
    let Some(key) = keys.get(idx) else {
        return Value::Null;
    };
    array_value_to_json(values, *key)
}

/// Map an Arrow `DataType` to the short label the portal expects in the
/// `columns[].data_type` slot. We keep it close to Arrow's `Display` so
/// existing UI code recognizes it.
fn arrow_data_type_label(dt: &DataType) -> String {
    match dt {
        DataType::Null => "Null".to_string(),
        DataType::Boolean => "Boolean".to_string(),
        DataType::Int8 => "Int8".to_string(),
        DataType::Int16 => "Int16".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::UInt8 => "UInt8".to_string(),
        DataType::UInt16 => "UInt16".to_string(),
        DataType::UInt32 => "UInt32".to_string(),
        DataType::UInt64 => "UInt64".to_string(),
        DataType::Float16 => "Float16".to_string(),
        DataType::Float32 => "Float32".to_string(),
        DataType::Float64 => "Float64".to_string(),
        DataType::Utf8 => "Utf8".to_string(),
        DataType::LargeUtf8 => "LargeUtf8".to_string(),
        DataType::Utf8View => "Utf8View".to_string(),
        DataType::Binary => "Binary".to_string(),
        DataType::LargeBinary => "LargeBinary".to_string(),
        DataType::BinaryView => "BinaryView".to_string(),
        DataType::FixedSizeBinary(n) => format!("FixedSizeBinary({n})"),
        DataType::Date32 => "Date32".to_string(),
        DataType::Date64 => "Date64".to_string(),
        DataType::Time32(unit) => format!("Time32({unit:?})"),
        DataType::Time64(unit) => format!("Time64({unit:?})"),
        DataType::Timestamp(unit, tz) => match tz {
            Some(tz) => format!("Timestamp({unit:?}, {tz})"),
            None => format!("Timestamp({unit:?})"),
        },
        DataType::Decimal128(p, s) => format!("Decimal128({p}, {s})"),
        DataType::Decimal256(p, s) => format!("Decimal256({p}, {s})"),
        DataType::List(field) => format!("List<{}>", arrow_data_type_label(field.data_type())),
        DataType::LargeList(field) => {
            format!("LargeList<{}>", arrow_data_type_label(field.data_type()))
        }
        DataType::FixedSizeList(field, n) => {
            format!("FixedSizeList<{}, {n}>", arrow_data_type_label(field.data_type()))
        }
        DataType::Struct(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), arrow_data_type_label(f.data_type())))
                .collect();
            format!("Struct<{}>", inner.join(", "))
        }
        DataType::Map(field, _) => format!("Map<{}>", arrow_data_type_label(field.data_type())),
        DataType::Dictionary(_, value) => arrow_data_type_label(value),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Int64Array, ListArray, NullArray, StringArray, StructArray,
        TimestampNanosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn encodes_common_scalar_types() {
        let s = schema(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("n", DataType::Null, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&s),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(NullArray::new(3)),
                // 1700000000 sec * 1e9 = a deterministic timestamp.
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_700_000_000_000_000_000),
                    None,
                    Some(0),
                ])),
            ],
        )
        .unwrap();

        let envelope = encode_record_batches(&[batch], 1000);
        let obj = envelope.as_object().unwrap();
        assert_eq!(obj["row_count"], Value::Number(Number::from(3)));
        assert_eq!(obj["truncated"], Value::Bool(false));

        let cols = obj["columns"].as_array().unwrap();
        assert_eq!(cols[0]["name"], "i");
        assert_eq!(cols[0]["data_type"], "Int64");
        assert_eq!(cols[1]["data_type"], "Utf8");
        assert_eq!(cols[2]["data_type"], "Boolean");
        assert_eq!(cols[3]["data_type"], "Null");
        assert_eq!(cols[4]["data_type"], "Timestamp(Nanosecond)");

        let rows = obj["rows"].as_array().unwrap();
        // row 0: 1, "a", true, null, "2023-11-14T22:13:20Z"
        assert_eq!(rows[0][0], Value::Number(Number::from(1)));
        assert_eq!(rows[0][1], Value::String("a".to_string()));
        assert_eq!(rows[0][2], Value::Bool(true));
        assert_eq!(rows[0][3], Value::Null);
        assert!(
            rows[0][4]
                .as_str()
                .map(|s| s.starts_with("2023-11-14"))
                .unwrap_or(false),
            "expected ISO 8601 timestamp, got {:?}",
            rows[0][4]
        );

        // row 1: null, "b", false, null, null
        assert_eq!(rows[1][0], Value::Null);
        assert_eq!(rows[1][1], Value::String("b".to_string()));
        assert_eq!(rows[1][2], Value::Bool(false));
        assert_eq!(rows[1][3], Value::Null);
        assert_eq!(rows[1][4], Value::Null);

        // row 2: 3, null, null, null, "1970-01-01T00:00:00Z"
        assert_eq!(rows[2][0], Value::Number(Number::from(3)));
        assert_eq!(rows[2][1], Value::Null);
        assert_eq!(rows[2][2], Value::Null);
        assert!(
            rows[2][4]
                .as_str()
                .map(|s| s.starts_with("1970-01-01"))
                .unwrap_or(false)
        );
    }

    #[test]
    fn encodes_list_and_struct() {
        // List<Int64>
        let list_field = Arc::new(Field::new("item", DataType::Int64, true));
        let values = Int64Array::from(vec![Some(10), Some(20), Some(30)]);
        let offsets = OffsetBuffer::new(vec![0_i32, 2, 3].into());
        let list_array = ListArray::new(Arc::clone(&list_field), offsets, Arc::new(values), None);

        // Struct<a: Int64, b: Utf8>
        let struct_fields: Vec<Arc<Field>> = vec![
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ];
        let a_arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let b_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), Some("y")]));
        let struct_array = StructArray::new(
            struct_fields.clone().into(),
            vec![a_arr, b_arr],
            None,
        );

        let s = schema(vec![
            Field::new(
                "lst",
                DataType::List(Arc::clone(&list_field)),
                true,
            ),
            Field::new("st", DataType::Struct(struct_fields.into()), true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&s),
            vec![Arc::new(list_array), Arc::new(struct_array)],
        )
        .unwrap();

        let envelope = encode_record_batches(&[batch], 1000);
        let rows = envelope["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);

        // row 0: [10, 20], {a: 1, b: "x"}
        let lst0 = rows[0][0].as_array().unwrap();
        assert_eq!(lst0.len(), 2);
        assert_eq!(lst0[0], Value::Number(Number::from(10)));
        assert_eq!(lst0[1], Value::Number(Number::from(20)));
        let st0 = rows[0][1].as_object().unwrap();
        assert_eq!(st0["a"], Value::Number(Number::from(1)));
        assert_eq!(st0["b"], Value::String("x".to_string()));

        // row 1: [30], {a: 2, b: "y"}
        let lst1 = rows[1][0].as_array().unwrap();
        assert_eq!(lst1.len(), 1);
        assert_eq!(lst1[0], Value::Number(Number::from(30)));
    }

    #[test]
    fn honors_row_cap() {
        let s = schema(vec![Field::new("i", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&s),
            vec![Arc::new(Int64Array::from((0..50_i64).collect::<Vec<_>>()))],
        )
        .unwrap();

        let envelope = encode_record_batches(&[batch], 10);
        assert_eq!(envelope["row_count"], Value::Number(Number::from(10)));
        assert_eq!(envelope["truncated"], Value::Bool(true));
        assert_eq!(envelope["rows"].as_array().unwrap().len(), 10);
    }

    #[test]
    fn honors_payload_byte_budget() {
        // Build a batch with one huge string per row so the byte budget
        // bites well before the row cap.
        let big = "x".repeat(64 * 1024);
        let values: Vec<Option<String>> = (0..1024).map(|_| Some(big.clone())).collect();
        let s = schema(vec![Field::new("s", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&s),
            vec![Arc::new(StringArray::from(values))],
        )
        .unwrap();

        let envelope = encode_record_batches(&[batch], 10_000);
        assert_eq!(envelope["truncated"], Value::Bool(true));
        // Some rows should land; not all 1024 should fit into 5 MiB once
        // strings are JSON-escaped.
        let n = envelope["row_count"].as_u64().unwrap();
        assert!(n > 0 && n < 1024, "row_count={n}");
    }

    #[test]
    fn binary_round_trips_as_base64() {
        let s = schema(vec![Field::new("blob", DataType::Binary, true)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&s),
            vec![Arc::new(BinaryArray::from(vec![
                Some(b"hello".as_ref()),
                Some(&[0xff, 0xee, 0xdd]),
            ]))],
        )
        .unwrap();
        let env = encode_record_batches(&[batch], 100);
        let rows = env["rows"].as_array().unwrap();
        assert_eq!(rows[0][0], Value::String("aGVsbG8=".to_string()));
        assert_eq!(rows[1][0], Value::String("/+7d".to_string()));
    }

    #[test]
    fn empty_input_yields_empty_envelope() {
        let env = encode_record_batches(&[], 100);
        assert_eq!(env["columns"].as_array().unwrap().len(), 0);
        assert_eq!(env["rows"].as_array().unwrap().len(), 0);
        assert_eq!(env["row_count"], Value::Number(Number::from(0)));
        assert_eq!(env["truncated"], Value::Bool(false));
    }
}
