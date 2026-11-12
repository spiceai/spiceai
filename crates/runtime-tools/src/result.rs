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

//! Bounded, compact results for the tools that return rows: `sql` and the sampler tools.
//!
//! A tool's result goes into a model's context window, so every result is cut to a
//! [`ResultBudget`]: at most [`DEFAULT_MAX_ROWS`] rows, and at most a byte budget sized from the
//! model's context window. A result that fits is `{"columns":[…],"rows":[[…],…]}`. A result that
//! does not is cut at the last whole row that fits and says so, with `truncated` and a `message`
//! telling the model not to compute totals from the rows it was shown.
//!
//! Every value means the same thing to any JSON parser as it does in Arrow:
//! - integers and finite floats are JSON numbers;
//! - a decimal with more significant digits than an `f64` holds exactly is a string, so no
//!   parser rounds it;
//! - a non-finite float is a string (`"NaN"`, `"Infinity"`, `"-Infinity"`) rather than the `null`
//!   `arrow-json` writes, which would read as SQL NULL.
//!
//! A long text value, a long list, or a binary value is shortened, with a marker saying how much
//! was left out, and the result's `message` says so.

use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::Display,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use arrow::{
    array::{Array, ArrayAccessor, AsArray, PrimitiveArray, RecordBatch},
    datatypes::{DataType, Float16Type, Float32Type, Float64Type, SchemaRef},
    util::display::{ArrayFormatter, FormatOptions},
};
use arrow_json::writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder};
use arrow_schema::{ArrowError, FieldRef};
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use serde_json::{Map, Value, json};

/// The most rows a tool result holds.
pub const DEFAULT_MAX_ROWS: usize = 100;
/// The largest byte budget, and the budget when the model's context window is unknown.
pub const MAX_RESULT_BYTES: usize = 32 * 1024;
/// The smallest byte budget, for models with a small context window.
pub const MIN_RESULT_BYTES: usize = 4 * 1024;
/// The characters of a text value kept before it is shortened.
pub const MAX_TEXT_CHARS: usize = 1_024;
/// The elements of a list kept before it is shortened.
pub const MAX_LIST_ITEMS: usize = 16;

/// Bytes per token used to turn a context window into a byte budget. Tokens of tabular JSON
/// average 2–4 bytes, so this errs toward a smaller result.
const BYTES_PER_TOKEN: u64 = 4;
/// A result may use one tenth of the model's context window.
const CONTEXT_WINDOW_SHARE: u64 = 10;
/// Every decimal with at most this many significant digits converts to the nearest `f64` and
/// back to the same digits (`f64::DIGITS`), so it is written as a JSON number. Wider decimals are
/// written as strings.
const MAX_EXACT_DECIMAL_PRECISION: u8 = 15;
/// Bytes of the byte budget kept for the result's other keys and its `message`.
const ENVELOPE_BYTES: usize = 640;

/// What the value encoders did to the values they wrote, which the result's `message` reports.
#[derive(Debug, Default)]
struct ValueFlags {
    /// A text value or a list was shortened.
    shortened: AtomicBool,
    /// A binary value was written as its size.
    binary: AtomicBool,
}

impl ValueFlags {
    fn snapshot(&self) -> (bool, bool) {
        (
            self.shortened.load(Ordering::Relaxed),
            self.binary.load(Ordering::Relaxed),
        )
    }

    fn restore(&self, (shortened, binary): (bool, bool)) {
        self.shortened.store(shortened, Ordering::Relaxed);
        self.binary.store(binary, Ordering::Relaxed);
    }
}

/// How much of a tool result a model may be given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultBudget {
    max_rows: usize,
    max_bytes: usize,
}

impl Default for ResultBudget {
    /// The budget for a caller whose context window is unknown: MCP clients and
    /// `POST /v1/tools/{name}`.
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            max_bytes: MAX_RESULT_BYTES,
        }
    }
}

impl ResultBudget {
    /// The budget for a model with a context window of `context_window_tokens`: one tenth of the
    /// window at four bytes per token, between [`MIN_RESULT_BYTES`] and [`MAX_RESULT_BYTES`].
    #[must_use]
    pub fn for_context_window(context_window_tokens: Option<u64>) -> Self {
        let Some(tokens) = context_window_tokens else {
            return Self::default();
        };
        let bytes = tokens.saturating_mul(BYTES_PER_TOKEN) / CONTEXT_WINDOW_SHARE;
        let max_bytes = usize::try_from(bytes)
            .unwrap_or(MAX_RESULT_BYTES)
            .clamp(MIN_RESULT_BYTES, MAX_RESULT_BYTES);
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            max_bytes,
        }
    }

    /// A budget with explicit bounds.
    #[must_use]
    pub fn with_limits(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            max_rows,
            max_bytes,
        }
    }

    #[must_use]
    pub fn max_rows(self) -> usize {
        self.max_rows
    }

    #[must_use]
    pub fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

tokio::task_local! {
    /// The budget of the model whose tool loop is running the current tool.
    static RESULT_BUDGET: ResultBudget;
}

/// The budget for the tool running now: the calling model's, when a tool loop set one with
/// [`with_budget`], or the default.
#[must_use]
pub fn current_budget() -> ResultBudget {
    RESULT_BUDGET.try_with(|budget| *budget).unwrap_or_default()
}

/// Runs `future`, a tool call, under `budget`.
pub async fn with_budget<F: Future>(budget: ResultBudget, future: F) -> F::Output {
    RESULT_BUDGET.scope(budget, future).await
}

/// The JSON Schema of a tool result, for MCP's `outputSchema`.
#[must_use]
pub fn result_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "columns": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Column names, in the order of each row's values."
            },
            "rows": {
                "type": "array",
                "items": {"type": "array"},
                "description": "The rows shown, each an array of values in `columns` order."
            },
            "truncated": {
                "type": "boolean",
                "description": "Present and true when the result has more rows than were shown."
            },
            "message": {
                "type": "string",
                "description": "Present when rows were left out or long values were shortened."
            }
        },
        "required": ["columns", "rows"]
    })
}

/// Rows encoded for a model, within a [`ResultBudget`].
#[derive(Debug)]
pub struct ToolRows {
    /// The column names shown: every column's, unless [`Self::too_wide`] says otherwise.
    columns: Vec<String>,
    /// The number of columns, when their names alone do not fit the budget. Then `columns` holds
    /// the names that fit followed by a marker, and no rows are shown.
    too_wide: Option<usize>,
    /// A JSON array of row arrays.
    rows: Vec<u8>,
    shown: usize,
    truncated: bool,
    shortened: bool,
    binary: bool,
    max_bytes: usize,
}

/// The `message` part saying what happened to long values.
const SHORTENED_VALUES: &str = "Some long values were shortened: text to its first 1,024 characters and lists to their first 16 items. Read the rest with substr(column, 1025, 1024) or array_slice(column, 17, 32).";
/// The `message` part saying how binary values are shown.
const BINARY_VALUES: &str =
    "Binary values are shown as their size; select encode(column, 'hex') to read one.";

impl ToolRows {
    /// The number of rows shown.
    #[must_use]
    pub fn shown(&self) -> usize {
        self.shown
    }

    /// Whether the result had rows beyond those shown.
    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.truncated
    }

    /// Whether a value shown was shortened.
    #[must_use]
    pub fn is_shortened(&self) -> bool {
        self.shortened
    }

    /// The message for the model, when rows were left out or values shortened or summarized.
    #[must_use]
    pub fn message(&self) -> Option<String> {
        let kib = self.max_bytes / 1024;
        let rows = if let Some(columns) = self.too_wide {
            Some(format!(
                "The result has {}, more than the {kib} KiB this result may use can name, so no rows are shown. Select fewer columns.",
                count_of(columns, "column")
            ))
        } else if !self.truncated {
            None
        } else if self.shown == 0 {
            Some(format!(
                "The first row is larger than the {kib} KiB this result may use, so no rows are shown. Select fewer columns, or select substr(column, 1, 1024) of long ones."
            ))
        } else {
            Some(format!(
                "Showing the first {} rows; the result has more. Do not compute totals, counts, or averages from these rows. Run an aggregate query instead, or narrow the query with a WHERE clause.",
                self.shown
            ))
        };
        let parts: Vec<&str> = [
            rows.as_deref(),
            self.shortened.then_some(SHORTENED_VALUES),
            self.binary.then_some(BINARY_VALUES),
        ]
        .into_iter()
        .flatten()
        .collect();
        (!parts.is_empty()).then(|| parts.join(" "))
    }

    /// The tool result as an object holding `rows`.
    fn object(&self, rows: Vec<Value>) -> Value {
        let mut object = Map::with_capacity(4);
        object.insert(
            "columns".to_string(),
            Value::Array(self.columns.iter().cloned().map(Value::String).collect()),
        );
        object.insert("rows".to_string(), Value::Array(rows));
        if self.truncated {
            object.insert("truncated".to_string(), Value::Bool(true));
        }
        if let Some(message) = self.message() {
            object.insert("message".to_string(), Value::String(message));
        }
        Value::Object(object)
    }

    /// The tool result: `{"columns":[…],"rows":[…]}`, with `truncated` when rows were left out and
    /// `message` when rows were left out or values shortened.
    ///
    /// The result, serialized, is never larger than the budget: should a row read back from JSON
    /// serialize longer than it was encoded (a whole `f64` gains `.0`), rows are dropped from the
    /// end, and the result is marked truncated, until it fits.
    ///
    /// # Errors
    ///
    /// Returns an error if the encoded rows are not valid JSON, which would be a bug in the
    /// encoder.
    pub fn into_value(mut self) -> Result<Value, serde_json::Error> {
        let mut rows: Vec<Value> = serde_json::from_slice(&self.rows)?;
        loop {
            let value = self.object(rows);
            if serde_json::to_vec(&value)?.len() <= self.max_bytes {
                return Ok(value);
            }
            let Value::Object(mut object) = value else {
                return Ok(value);
            };
            rows = match object.remove("rows") {
                Some(Value::Array(rows)) if !rows.is_empty() => rows,
                _ => return Ok(Value::Object(object_with_rows(object))),
            };
            rows.pop();
            self.shown = rows.len();
            self.truncated = true;
        }
    }
}

/// `object` with an empty `rows` put back.
fn object_with_rows(mut object: Map<String, Value>) -> Map<String, Value> {
    object.insert("rows".to_string(), Value::Array(Vec::new()));
    object
}

/// `count` and `noun`, plural unless `count` is 1.
fn count_of(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// The length of `value` serialized as JSON.
fn json_len(value: &impl serde::Serialize) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |json| json.len())
}

/// The longest prefix of `names` that fits in `limit` bytes as a JSON array together with a
/// marker of how many names were left out.
fn names_that_fit(names: &[String], limit: usize) -> Vec<String> {
    let mut shown: Vec<String> = Vec::new();
    for name in names {
        shown.push(name.clone());
        let marker = format!("…[+{}]", count_of(names.len() - shown.len(), "column"));
        shown.push(marker);
        let fits = json_len(&shown) <= limit;
        shown.pop();
        if !fits {
            shown.pop();
            break;
        }
    }
    let left_out = names.len() - shown.len();
    if left_out > 0 {
        shown.push(format!("…[+{}]", count_of(left_out, "column")));
    }
    shown
}

/// Whether a [`RowEncoder`] can take more rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// The budget has room for more rows.
    Open,
    /// The budget is spent: the result is complete or truncated, and no more rows are read.
    Full,
}

/// Encodes record batches into a [`ToolRows`], one row at a time, until the budget is spent.
pub struct RowEncoder {
    columns: Vec<String>,
    too_wide: Option<usize>,
    budget: ResultBudget,
    /// The bytes the encoded rows may take: the budget less the column names and the envelope.
    rows_budget: usize,
    rows: Vec<u8>,
    row: Vec<u8>,
    shown: usize,
    truncated: bool,
    flags: Arc<ValueFlags>,
    options: EncoderOptions,
}

impl RowEncoder {
    #[must_use]
    pub fn new(schema: &SchemaRef, budget: ResultBudget) -> Self {
        let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        // The column names are measured as they are serialized, escapes included.
        let names_bytes = json_len(&names);
        let (columns, too_wide, rows_budget) = if let Some(rows_budget) = budget
            .max_bytes
            .checked_sub(names_bytes.saturating_add(ENVELOPE_BYTES))
        {
            (names, None, rows_budget)
        } else {
            let total = names.len();
            let limit = budget.max_bytes.saturating_sub(ENVELOPE_BYTES);
            (names_that_fit(&names, limit), Some(total), 0)
        };
        let flags = Arc::new(ValueFlags::default());
        let options = EncoderOptions::default()
            .with_explicit_nulls(true)
            .with_encoder_factory(Arc::new(ModelValueEncoders {
                flags: Arc::clone(&flags),
            }));
        let mut rows = Vec::with_capacity(rows_budget.min(MAX_RESULT_BYTES));
        rows.push(b'[');
        Self {
            columns,
            too_wide,
            budget,
            rows_budget,
            rows,
            row: Vec::new(),
            shown: 0,
            truncated: false,
            flags,
            options,
        }
    }

    /// Encodes the rows of `batch` until the budget is spent.
    ///
    /// # Errors
    ///
    /// Returns an error if a column's type has no JSON encoding.
    pub fn push(&mut self, batch: &RecordBatch) -> Result<Progress, ArrowError> {
        if self.truncated {
            return Ok(Progress::Full);
        }
        if batch.num_rows() == 0 {
            return Ok(Progress::Open);
        }
        let schema = batch.schema();
        let options = &self.options;
        let mut encoders = schema
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, array)| make_encoder(field, array.as_ref(), options))
            .collect::<Result<Vec<_>, _>>()?;

        for row in 0..batch.num_rows() {
            // Reaching the row limit with a row still to read is how a result is known to have
            // more rows than it shows.
            if self.shown == self.budget.max_rows {
                self.truncated = true;
                return Ok(Progress::Full);
            }
            let flags_before = self.flags.snapshot();
            self.row.clear();
            if self.shown > 0 {
                self.row.push(b',');
            }
            self.row.push(b'[');
            for (column, encoder) in encoders.iter_mut().enumerate() {
                if column > 0 {
                    self.row.push(b',');
                }
                if encoder.is_null(row) {
                    self.row.extend_from_slice(b"null");
                } else {
                    encoder.encode(row, &mut self.row);
                }
            }
            self.row.push(b']');
            // `+ 1` for the array's closing bracket.
            if self.rows.len() + self.row.len() + 1 > self.rows_budget {
                // A value shortened only in a row that is not shown was not shortened for the
                // model.
                self.flags.restore(flags_before);
                self.truncated = true;
                return Ok(Progress::Full);
            }
            self.rows.extend_from_slice(&self.row);
            self.shown += 1;
        }
        Ok(Progress::Open)
    }

    /// The encoded result.
    #[must_use]
    pub fn finish(mut self) -> ToolRows {
        self.rows.push(b']');
        let (shortened, binary) = self.flags.snapshot();
        ToolRows {
            columns: self.columns,
            too_wide: self.too_wide,
            rows: self.rows,
            shown: self.shown,
            truncated: self.truncated,
            shortened,
            binary,
            max_bytes: self.budget.max_bytes,
        }
    }
}

/// Encodes the rows of `stream` within `budget`. Stops reading, which stops the query, as soon as
/// the budget is spent: one row past the budget is read to know whether the result has more.
///
/// # Errors
///
/// Returns an error if the query fails or a column's type has no JSON encoding.
pub async fn encode_stream(
    mut stream: SendableRecordBatchStream,
    budget: ResultBudget,
) -> Result<ToolRows, Box<dyn StdError + Send + Sync>> {
    let mut encoder = RowEncoder::new(&stream.schema(), budget);
    while let Some(batch) = stream.next().await {
        if encoder.push(&batch?)? == Progress::Full {
            break;
        }
    }
    Ok(encoder.finish())
}

/// Encodes the rows of `batches` within `budget`.
///
/// # Errors
///
/// Returns an error if a column's type has no JSON encoding.
pub fn encode_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    budget: ResultBudget,
) -> Result<ToolRows, ArrowError> {
    let mut encoder = RowEncoder::new(schema, budget);
    for batch in batches {
        if encoder.push(batch)? == Progress::Full {
            break;
        }
    }
    Ok(encoder.finish())
}

/// The value encoders that differ from `arrow-json`'s, for every column and every nested value.
#[derive(Debug)]
struct ModelValueEncoders {
    flags: Arc<ValueFlags>,
}

impl EncoderFactory for ModelValueEncoders {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let flags = || Arc::clone(&self.flags);
        let encoder: Box<dyn Encoder + 'a> = match array.data_type() {
            DataType::Utf8 => Box::new(TextEncoder {
                values: array.as_string::<i32>(),
                flags: flags(),
            }),
            DataType::LargeUtf8 => Box::new(TextEncoder {
                values: array.as_string::<i64>(),
                flags: flags(),
            }),
            DataType::Utf8View => Box::new(TextEncoder {
                values: array.as_string_view(),
                flags: flags(),
            }),
            DataType::Binary => Box::new(BinaryEncoder {
                values: array.as_binary::<i32>(),
                flags: flags(),
            }),
            DataType::LargeBinary => Box::new(BinaryEncoder {
                values: array.as_binary::<i64>(),
                flags: flags(),
            }),
            DataType::BinaryView => Box::new(BinaryEncoder {
                values: array.as_binary_view(),
                flags: flags(),
            }),
            DataType::FixedSizeBinary(_) => Box::new(BinaryEncoder {
                values: array.as_fixed_size_binary(),
                flags: flags(),
            }),
            DataType::Float16 => Box::new(FloatEncoder {
                values: array.as_primitive::<Float16Type>(),
            }),
            DataType::Float32 => Box::new(FloatEncoder {
                values: array.as_primitive::<Float32Type>(),
            }),
            DataType::Float64 => Box::new(FloatEncoder {
                values: array.as_primitive::<Float64Type>(),
            }),
            DataType::Decimal32(precision, _)
            | DataType::Decimal64(precision, _)
            | DataType::Decimal128(precision, _)
            | DataType::Decimal256(precision, _)
                if *precision > MAX_EXACT_DECIMAL_PRECISION =>
            {
                Box::new(QuotedDisplayEncoder {
                    formatter: ArrayFormatter::try_new(array, &FormatOptions::default())?,
                })
            }
            // `arrow-json` has no encoding for a union; its display form names the variant.
            DataType::Union(_, _) => Box::new(QuotedDisplayEncoder {
                formatter: ArrayFormatter::try_new(array, &FormatOptions::default())?,
            }),
            DataType::List(item) => {
                let list = array.as_list::<i32>();
                Box::new(ListEncoder {
                    spans: offset_spans(list.value_offsets())?,
                    values: make_encoder(item, list.values().as_ref(), options)?,
                    flags: flags(),
                })
            }
            DataType::LargeList(item) => {
                let list = array.as_list::<i64>();
                Box::new(ListEncoder {
                    spans: offset_spans(list.value_offsets())?,
                    values: make_encoder(item, list.values().as_ref(), options)?,
                    flags: flags(),
                })
            }
            DataType::ListView(item) => {
                let list = array.as_list_view::<i32>();
                Box::new(ListEncoder {
                    spans: view_spans(list.value_offsets(), list.value_sizes())?,
                    values: make_encoder(item, list.values().as_ref(), options)?,
                    flags: flags(),
                })
            }
            DataType::LargeListView(item) => {
                let list = array.as_list_view::<i64>();
                Box::new(ListEncoder {
                    spans: view_spans(list.value_offsets(), list.value_sizes())?,
                    values: make_encoder(item, list.values().as_ref(), options)?,
                    flags: flags(),
                })
            }
            DataType::FixedSizeList(item, size) => {
                let list = array.as_fixed_size_list();
                let size = offset_to_usize(*size)?;
                Box::new(ListEncoder {
                    spans: (0..list.len()).map(|i| (i * size, size)).collect(),
                    values: make_encoder(item, list.values().as_ref(), options)?,
                    flags: flags(),
                })
            }
            // A map is a list of `[key, value]` pairs, not a JSON object: keys of any type stay
            // typed, and two keys that read the same once shortened stay two entries.
            DataType::Map(entries, _) => {
                let DataType::Struct(entry_fields) = entries.data_type() else {
                    return Ok(None);
                };
                let [key_field, value_field] = entry_fields.as_ref() else {
                    return Ok(None);
                };
                let map = array.as_map();
                Box::new(MapEncoder {
                    spans: offset_spans(map.value_offsets())?,
                    keys: make_encoder(key_field, map.keys().as_ref(), options)?,
                    values: make_encoder(value_field, map.values().as_ref(), options)?,
                    flags: flags(),
                })
            }
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, array.nulls().cloned())))
    }
}

fn offset_to_usize<O: TryInto<usize> + Copy + Display>(offset: O) -> Result<usize, ArrowError> {
    offset.try_into().map_err(|_| {
        ArrowError::InvalidArgumentError(format!("List offset {offset} is not a valid index"))
    })
}

/// Each list's start and length in its values, from offsets where list `i` is
/// `offsets[i]..offsets[i + 1]`.
fn offset_spans<O: TryInto<usize> + Copy + Display>(
    offsets: &[O],
) -> Result<Vec<(usize, usize)>, ArrowError> {
    offsets
        .windows(2)
        .map(|pair| {
            let start = offset_to_usize(pair[0])?;
            let end = offset_to_usize(pair[1])?;
            Ok((start, end.saturating_sub(start)))
        })
        .collect()
}

/// Each list view's start and length in its values.
fn view_spans<O: TryInto<usize> + Copy + Display>(
    offsets: &[O],
    sizes: &[O],
) -> Result<Vec<(usize, usize)>, ArrowError> {
    offsets
        .iter()
        .zip(sizes)
        .map(|(offset, size)| Ok((offset_to_usize(*offset)?, offset_to_usize(*size)?)))
        .collect()
}

/// Writes `value` as a JSON string.
fn write_json_string(value: &str, out: &mut Vec<u8>) {
    let start = out.len();
    if serde_json::to_writer(&mut *out, value).is_err() {
        // Writing a `str` into a `Vec` cannot fail; `null` keeps the output valid JSON regardless.
        out.truncate(start);
        out.extend_from_slice(b"null");
    }
}

/// A text value, shortened to [`MAX_TEXT_CHARS`] characters with a marker of how many were left
/// out.
struct TextEncoder<A> {
    values: A,
    flags: Arc<ValueFlags>,
}

impl<'a, A> Encoder for TextEncoder<A>
where
    A: ArrayAccessor<Item = &'a str>,
{
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = self.values.value(idx);
        match value.char_indices().nth(MAX_TEXT_CHARS) {
            None => write_json_string(value, out),
            Some((cut, _)) => {
                let left_out = value[cut..].chars().count();
                self.flags.shortened.store(true, Ordering::Relaxed);
                write_json_string(&format!("{}…[+{left_out} chars]", &value[..cut]), out);
            }
        }
    }
}

/// A binary value, as its length.
struct BinaryEncoder<A> {
    values: A,
    flags: Arc<ValueFlags>,
}

impl<'a, A> Encoder for BinaryEncoder<A>
where
    A: ArrayAccessor<Item = &'a [u8]>,
{
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let len = self.values.value(idx).len();
        self.flags.binary.store(true, Ordering::Relaxed);
        write_json_string(&format!("<binary {len} bytes>"), out);
    }
}

/// A float as a JSON number, or as a string when it is not finite.
struct FloatEncoder<'a, T: arrow::datatypes::ArrowPrimitiveType> {
    values: &'a PrimitiveArray<T>,
}

/// Writes a float as the shortest decimal that reads back as the same value, or as a string when
/// it is not finite, so that no JSON parser reads it as `null`.
fn write_float(value: impl Into<f64> + Copy + serde::Serialize, out: &mut Vec<u8>) {
    let wide: f64 = value.into();
    if wide.is_nan() {
        out.extend_from_slice(b"\"NaN\"");
    } else if wide.is_infinite() {
        out.extend_from_slice(if wide > 0.0 {
            b"\"Infinity\""
        } else {
            b"\"-Infinity\""
        });
    } else {
        // `serde_json` writes a finite float as the shortest decimal that round-trips for the
        // value's own width (`0.1` for an `f32` 0.1, not its `f64` widening).
        let start = out.len();
        if serde_json::to_writer(&mut *out, &value).is_err() {
            out.truncate(start);
            out.extend_from_slice(b"null");
        }
    }
}

impl Encoder for FloatEncoder<'_, Float64Type> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        write_float(self.values.value(idx), out);
    }
}

impl Encoder for FloatEncoder<'_, Float32Type> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        write_float(self.values.value(idx), out);
    }
}

impl Encoder for FloatEncoder<'_, Float16Type> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        write_float(self.values.value(idx).to_f32(), out);
    }
}

/// A value written as a JSON string of its display form: a decimal too wide for an `f64`, or a
/// type `arrow-json` cannot encode.
struct QuotedDisplayEncoder<'a> {
    formatter: ArrayFormatter<'a>,
}

impl Encoder for QuotedDisplayEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        write_json_string(&self.formatter.value(idx).to_string(), out);
    }
}

/// Writes the marker for `left_out` elements that were not written, after `written` that were.
fn write_left_out(written: usize, left_out: usize, unit: &str, out: &mut Vec<u8>) {
    if written > 0 {
        out.push(b',');
    }
    write_json_string(&format!("…[+{left_out} {unit}]"), out);
}

/// A list, shortened to [`MAX_LIST_ITEMS`] elements with a marker of how many were left out.
struct ListEncoder<'a> {
    /// Each list's start and length in `values`.
    spans: Vec<(usize, usize)>,
    values: NullableEncoder<'a>,
    flags: Arc<ValueFlags>,
}

impl Encoder for ListEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let (start, len) = self.spans.get(idx).copied().unwrap_or_default();
        let shown = len.min(MAX_LIST_ITEMS);
        out.push(b'[');
        for (n, value) in (start..start + shown).enumerate() {
            if n > 0 {
                out.push(b',');
            }
            if self.values.is_null(value) {
                out.extend_from_slice(b"null");
            } else {
                self.values.encode(value, out);
            }
        }
        if len > shown {
            self.flags.shortened.store(true, Ordering::Relaxed);
            write_left_out(shown, len - shown, "items", out);
        }
        out.push(b']');
    }
}

/// A map, as a list of `[key, value]` pairs, shortened to [`MAX_LIST_ITEMS`] entries.
struct MapEncoder<'a> {
    /// Each map's start and number of entries.
    spans: Vec<(usize, usize)>,
    keys: NullableEncoder<'a>,
    values: NullableEncoder<'a>,
    flags: Arc<ValueFlags>,
}

impl Encoder for MapEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let (start, len) = self.spans.get(idx).copied().unwrap_or_default();
        let shown = len.min(MAX_LIST_ITEMS);
        out.push(b'[');
        for (n, entry) in (start..start + shown).enumerate() {
            if n > 0 {
                out.push(b',');
            }
            out.push(b'[');
            if self.keys.is_null(entry) {
                out.extend_from_slice(b"null");
            } else {
                self.keys.encode(entry, out);
            }
            out.push(b',');
            if self.values.is_null(entry) {
                out.extend_from_slice(b"null");
            } else {
                self.values.encode(entry, out);
            }
            out.push(b']');
        }
        if len > shown {
            self.flags.shortened.store(true, Ordering::Relaxed);
            write_left_out(shown, len - shown, "entries", out);
        }
        out.push(b']');
    }
}

/// The text a model is given for a tool result: a string as it is, anything else as compact
/// JSON. A string re-encoded as JSON would reach the model as an escaped string literal.
#[must_use]
pub fn tool_result_text(value: &Value) -> Cow<'_, str> {
    match value {
        Value::String(text) => Cow::Borrowed(text.as_str()),
        other => Cow::Owned(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow::{
        array::{
            ArrayRef, BinaryArray, Decimal128Array, Float32Array, Float64Array, Int32Array,
            Int32Builder, Int64Array, ListArray, ListViewArray, MapBuilder, StringArray,
            StringBuilder, StructArray, UnionArray,
        },
        buffer::ScalarBuffer,
        datatypes::{Field, Int32Type, UnionFields},
    };
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    use super::*;

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).expect("valid batch")
    }

    /// The tool result for `batches`. Compared as a `Value`, since the order of an object's keys
    /// depends on whether `serde_json`'s `preserve_order` is enabled in the build.
    fn encode(batches: &[RecordBatch], budget: ResultBudget) -> Value {
        encode_batches(&batches[0].schema(), batches, budget)
            .expect("encodes")
            .into_value()
            .expect("valid JSON")
    }

    const MORE_ROWS: &str = "Do not compute totals, counts, or averages from these rows. Run an aggregate query instead, or narrow the query with a WHERE clause.";

    fn ids(n: i64) -> RecordBatch {
        batch(vec![(
            "id",
            Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())) as ArrayRef,
        )])
    }

    #[test]
    fn a_result_that_fits_has_only_columns_and_rows() {
        let rows = batch(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            (
                "name",
                Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            ),
        ]);
        assert_eq!(
            encode(&[rows], ResultBudget::default()),
            json!({"columns": ["id", "name"], "rows": [[1, "a"], [2, null]]})
        );
    }

    #[test]
    fn a_result_over_the_row_limit_is_cut_and_says_so() {
        assert_eq!(
            encode(&[ids(3)], ResultBudget::with_limits(2, MAX_RESULT_BYTES)),
            json!({
                "columns": ["id"],
                "rows": [[0], [1]],
                "truncated": true,
                "message": format!("Showing the first 2 rows; the result has more. {MORE_ROWS}"),
            })
        );
    }

    #[test]
    fn a_result_of_exactly_the_row_limit_is_not_truncated() {
        assert_eq!(
            encode(&[ids(2)], ResultBudget::with_limits(2, MAX_RESULT_BYTES)),
            json!({"columns": ["id"], "rows": [[0], [1]]})
        );
    }

    #[test]
    fn rows_left_in_a_later_batch_truncate_the_result() {
        let empty = ids(0);
        assert_eq!(
            encode(
                &[ids(2), empty, ids(1)],
                ResultBudget::with_limits(2, MAX_RESULT_BYTES)
            ),
            json!({
                "columns": ["id"],
                "rows": [[0], [1]],
                "truncated": true,
                "message": format!("Showing the first 2 rows; the result has more. {MORE_ROWS}"),
            })
        );
    }

    #[test]
    fn the_byte_budget_cuts_at_the_last_whole_row() {
        // The rows may take 64 bytes less the column name (`"id"`, 5 bytes with its comma) and
        // the array's brackets: 57. `[0]` is 3 bytes, `,[1]` .. `,[9]` 4 each, and `,[10]` ..
        // 5 each, so rows 0 to 12 take 56 bytes with the closing bracket and row 13 would not
        // fit.
        let rows = encode_batches(
            &ids(1_000).schema(),
            &[ids(1_000)],
            ResultBudget::with_limits(1_000, ENVELOPE_BYTES + 64),
        )
        .expect("encodes");
        assert!(rows.is_truncated());
        assert_eq!(rows.shown(), 13);
        assert_eq!(
            rows.into_value().expect("valid JSON")["rows"],
            json!([
                [0],
                [1],
                [2],
                [3],
                [4],
                [5],
                [6],
                [7],
                [8],
                [9],
                [10],
                [11],
                [12]
            ])
        );
    }

    #[test]
    fn a_first_row_larger_than_the_budget_shows_no_rows() {
        let long = || Arc::new(StringArray::from(vec!["x".repeat(1_000)])) as ArrayRef;
        let wide = batch(vec![
            ("a", long()),
            ("b", long()),
            ("c", long()),
            ("d", long()),
            ("e", long()),
        ]);
        assert_eq!(
            encode(&[wide], ResultBudget::with_limits(10, MIN_RESULT_BYTES)),
            json!({
                "columns": ["a", "b", "c", "d", "e"],
                "rows": [],
                "truncated": true,
                "message": "The first row is larger than the 4 KiB this result may use, so no rows are shown. Select fewer columns, or select substr(column, 1, 1024) of long ones.",
            })
        );
    }

    fn serialized_len(value: &Value) -> usize {
        serde_json::to_vec(value).expect("serializes").len()
    }

    #[test]
    fn column_names_alone_never_exceed_the_budget() {
        // One name longer than the whole budget, and no rows.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "x".repeat(40_000),
            DataType::Int32,
            true,
        )]));
        let value = encode_batches(&schema, &[], ResultBudget::default())
            .expect("encodes")
            .into_value()
            .expect("valid JSON");
        assert!(
            serialized_len(&value) <= MAX_RESULT_BYTES,
            "{}",
            serialized_len(&value)
        );
        assert_eq!(value["columns"], json!(["…[+1 column]"]));
        assert_eq!(value["rows"], json!([]));
        assert_eq!(
            value["message"],
            "The result has 1 column, more than the 32 KiB this result may use can name, so no rows are shown. Select fewer columns."
        );
        assert!(
            value.get("truncated").is_none(),
            "there were no rows to leave out"
        );

        // Names that only escaping makes too long, with a row.
        let names: Vec<Field> = (0..20)
            .map(|i| Field::new(format!("{i}{}", "\u{1}".repeat(400)), DataType::Int32, true))
            .collect();
        let schema = Arc::new(arrow::datatypes::Schema::new(names));
        let columns: Vec<ArrayRef> = (0..20)
            .map(|_| Arc::new(Int32Array::from(vec![1])) as ArrayRef)
            .collect();
        let row = RecordBatch::try_new(Arc::clone(&schema), columns).expect("valid batch");
        let value = encode_batches(&schema, &[row], ResultBudget::default())
            .expect("encodes")
            .into_value()
            .expect("valid JSON");
        assert!(
            serialized_len(&value) <= MAX_RESULT_BYTES,
            "{}",
            serialized_len(&value)
        );
        assert_eq!(value["rows"], json!([]));
        assert_eq!(value["truncated"], json!(true));
        let columns = value["columns"].as_array().expect("an array");
        assert!(
            columns
                .last()
                .and_then(Value::as_str)
                .is_some_and(|marker| marker.starts_with("…[+") && marker.ends_with(" columns]")),
            "{columns:?}"
        );
    }

    #[test]
    fn a_result_never_serializes_larger_than_its_budget() {
        // Control characters take six bytes each once escaped.
        let escaped = "\u{1}".repeat(300);
        let rows = batch(vec![
            (
                "id",
                Arc::new(Int64Array::from((0..500).collect::<Vec<i64>>())) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![escaped.as_str(); 500])) as ArrayRef,
            ),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from((0..500).map(|i| i * 100_000_000).collect::<Vec<i128>>())
                        .with_precision_and_scale(15, 2)
                        .expect("valid decimal"),
                ) as ArrayRef,
            ),
        ]);
        for max_bytes in [MIN_RESULT_BYTES, 8 * 1024, 13_107, MAX_RESULT_BYTES] {
            let value = encode(
                std::slice::from_ref(&rows),
                ResultBudget::with_limits(DEFAULT_MAX_ROWS, max_bytes),
            );
            assert!(
                serialized_len(&value) <= max_bytes,
                "{} > {max_bytes}",
                serialized_len(&value)
            );
            assert_eq!(value["truncated"], json!(true));
        }
    }

    #[test]
    fn decimals_keep_every_digit() {
        let narrow = Decimal128Array::from(vec![Some(1250), None])
            .with_precision_and_scale(10, 2)
            .expect("valid decimal");
        let wide = Decimal128Array::from(vec![Some(12_345_678_901_234_567_890_123_456_789), None])
            .with_precision_and_scale(38, 10)
            .expect("valid decimal");
        assert_eq!(
            encode(
                &[batch(vec![
                    ("narrow", Arc::new(narrow) as ArrayRef),
                    ("wide", Arc::new(wide) as ArrayRef),
                ])],
                ResultBudget::default()
            ),
            json!({
                "columns": ["narrow", "wide"],
                "rows": [[12.5, "1234567890123456789.0123456789"], [null, null]],
            })
        );
    }

    #[test]
    fn non_finite_floats_are_strings_not_null() {
        let doubles = Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
            Some(0.1),
        ]);
        let floats = Float32Array::from(vec![
            Some(f32::NAN),
            Some(0.1),
            Some(1e21),
            None,
            Some(-2.5e-8),
        ]);
        assert_eq!(
            encode(
                &[batch(vec![
                    ("double", Arc::new(doubles) as ArrayRef),
                    ("float", Arc::new(floats) as ArrayRef),
                ])],
                ResultBudget::default()
            ),
            json!({
                "columns": ["double", "float"],
                "rows": [
                    ["NaN", "NaN"],
                    ["Infinity", 0.1],
                    ["-Infinity", 1e21],
                    [null, null],
                    [0.1, -2.5e-8],
                ],
            })
        );
    }

    #[test]
    fn long_text_lists_and_binary_are_shortened_and_marked() {
        let text = StringArray::from(vec![format!("{}é", "a".repeat(MAX_TEXT_CHARS + 2))]);
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(
            (0..20).map(Some).collect::<Vec<_>>(),
        )]);
        let bytes = BinaryArray::from(vec![b"abc".as_slice()]);
        let value = encode(
            &[batch(vec![
                ("t", Arc::new(text) as ArrayRef),
                ("l", Arc::new(list) as ArrayRef),
                ("b", Arc::new(bytes) as ArrayRef),
            ])],
            ResultBudget::default(),
        );
        let row = &value["rows"][0];
        assert_eq!(
            row[0],
            Value::String(format!("{}…[+3 chars]", "a".repeat(MAX_TEXT_CHARS)))
        );
        assert_eq!(
            row[1],
            json!([
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                "…[+4 items]"
            ])
        );
        assert_eq!(row[2], Value::String("<binary 3 bytes>".to_string()));
        assert_eq!(
            value["message"],
            Value::String(format!("{SHORTENED_VALUES} {BINARY_VALUES}"))
        );
        assert!(value.get("truncated").is_none());
    }

    #[test]
    fn binary_alone_says_how_to_read_it() {
        let bytes = BinaryArray::from(vec![b"abc".as_slice()]);
        assert_eq!(
            encode(
                &[batch(vec![("b", Arc::new(bytes) as ArrayRef)])],
                ResultBudget::default()
            ),
            json!({
                "columns": ["b"],
                "rows": [["<binary 3 bytes>"]],
                "message": "Binary values are shown as their size; select encode(column, 'hex') to read one.",
            })
        );
    }

    #[test]
    fn map_entries_stay_distinct_and_keep_their_key_types() {
        // Two keys that read the same once shortened.
        let mut text_keys = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        text_keys
            .keys()
            .append_value(format!("{}X", "a".repeat(MAX_TEXT_CHARS)));
        text_keys.values().append_value(1);
        text_keys
            .keys()
            .append_value(format!("{}Y", "a".repeat(MAX_TEXT_CHARS)));
        text_keys.values().append_value(2);
        text_keys.append(true).expect("appends");
        // Integer keys, which `arrow-json` cannot write as object keys.
        let mut int_keys = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
        int_keys.keys().append_value(1);
        int_keys.values().append_value("one");
        int_keys.append(true).expect("appends");

        let value = encode(
            &[batch(vec![
                ("text_keys", Arc::new(text_keys.finish()) as ArrayRef),
                ("int_keys", Arc::new(int_keys.finish()) as ArrayRef),
            ])],
            ResultBudget::default(),
        );
        let shortened = format!("{}…[+1 chars]", "a".repeat(MAX_TEXT_CHARS));
        assert_eq!(
            value["rows"][0],
            json!([[[shortened, 1], [shortened, 2]], [[1, "one"]]])
        );
    }

    #[test]
    fn list_views_are_shortened_like_lists() {
        let values = Int32Array::from((0..20).collect::<Vec<i32>>());
        let view = ListViewArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            ScalarBuffer::from(vec![0]),
            ScalarBuffer::from(vec![20]),
            Arc::new(values),
            None,
        );
        let value = encode(
            &[batch(vec![("v", Arc::new(view) as ArrayRef)])],
            ResultBudget::default(),
        );
        assert_eq!(
            value["rows"][0][0],
            json!([
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                "…[+4 items]"
            ])
        );
    }

    #[test]
    fn a_union_is_written_as_its_display_form() {
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("n", DataType::Int32, true),
                Field::new("s", DataType::Utf8, true),
            ],
        )
        .expect("valid union fields");
        let union = UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8, 1]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![None, Some("seven")])) as ArrayRef,
            ],
        )
        .expect("valid union");
        let value = encode(
            &[batch(vec![("u", Arc::new(union) as ArrayRef)])],
            ResultBudget::default(),
        );
        assert_eq!(value["rows"], json!([["{n=7}"], ["{s=seven}"]]));
    }

    #[test]
    fn floats_and_decimals_read_back_exactly() {
        let floats = Float64Array::from(vec![
            1e-30,
            0.1 + 0.2,
            f64::MAX,
            f64::MIN_POSITIVE,
            5e-324,
            -0.0,
        ]);
        let value = encode(
            &[batch(vec![("f", Arc::new(floats.clone()) as ArrayRef)])],
            ResultBudget::default(),
        );
        for (i, expected) in floats.values().iter().enumerate() {
            let read = value["rows"][i][0].as_f64().expect("a number");
            assert_eq!(
                read.to_bits(),
                expected.to_bits(),
                "row {i}: {read} vs {expected}"
            );
        }

        // A negative scale, and a scale equal to the precision, each within 15 digits.
        let negative_scale = Decimal128Array::from(vec![999_999_999_999_999_i128])
            .with_precision_and_scale(15, -8)
            .expect("valid decimal");
        let deep_scale = Decimal128Array::from(vec![12_345_i128])
            .with_precision_and_scale(10, 10)
            .expect("valid decimal");
        let value = encode(
            &[batch(vec![
                ("negative_scale", Arc::new(negative_scale) as ArrayRef),
                ("deep_scale", Arc::new(deep_scale) as ArrayRef),
            ])],
            ResultBudget::default(),
        );
        assert_eq!(
            value["rows"][0][0].as_f64(),
            Some(99_999_999_999_999_900_000_000.0)
        );
        assert_eq!(value["rows"][0][1].as_f64(), Some(0.000_001_234_5));
    }

    #[test]
    fn text_inside_a_struct_is_shortened_too() {
        let inner = StringArray::from(vec!["b".repeat(MAX_TEXT_CHARS + 1)]);
        let nested = StructArray::from(vec![(
            Arc::new(Field::new("doc", DataType::Utf8, false)),
            Arc::new(inner) as ArrayRef,
        )]);
        let value = encode_batches(
            &batch(vec![("s", Arc::new(nested.clone()) as ArrayRef)]).schema(),
            &[batch(vec![("s", Arc::new(nested) as ArrayRef)])],
            ResultBudget::default(),
        )
        .expect("encodes")
        .into_value()
        .expect("valid JSON");
        assert_eq!(
            value["rows"][0][0]["doc"],
            Value::String(format!("{}…[+1 chars]", "b".repeat(MAX_TEXT_CHARS)))
        );
        assert!(
            value["message"]
                .as_str()
                .is_some_and(|m| m.contains("shortened"))
        );
    }

    #[test]
    fn a_value_shortened_only_in_a_row_left_out_is_not_reported() {
        let text = StringArray::from(vec!["short".to_string(), "c".repeat(5_000)]);
        let rows = encode_batches(
            &batch(vec![("t", Arc::new(text.clone()) as ArrayRef)]).schema(),
            &[batch(vec![("t", Arc::new(text) as ArrayRef)])],
            ResultBudget::with_limits(1, MAX_RESULT_BYTES),
        )
        .expect("encodes");
        assert!(rows.is_truncated());
        assert!(!rows.is_shortened());
    }

    #[tokio::test]
    async fn encoding_stops_reading_once_the_budget_is_spent() {
        let first = ids(10);
        let schema = first.schema();
        let endless = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter([Ok(first)]).chain(futures::stream::pending()),
        ));
        let rows = tokio::time::timeout(
            Duration::from_secs(10),
            encode_stream(endless, ResultBudget::with_limits(5, MAX_RESULT_BYTES)),
        )
        .await
        .expect("the budget stops reading the stream")
        .expect("encodes");
        assert_eq!(rows.shown(), 5);
        assert!(rows.is_truncated());
    }

    #[test]
    fn the_byte_budget_is_a_tenth_of_the_context_window_within_bounds() {
        assert_eq!(
            ResultBudget::for_context_window(None).max_bytes(),
            32 * 1024
        );
        assert_eq!(
            ResultBudget::for_context_window(Some(1_000_000)).max_bytes(),
            32 * 1024
        );
        assert_eq!(
            ResultBudget::for_context_window(Some(32_768)).max_bytes(),
            13_107
        );
        assert_eq!(
            ResultBudget::for_context_window(Some(4_096)).max_bytes(),
            4 * 1024
        );
        assert_eq!(
            ResultBudget::for_context_window(Some(200_000)).max_rows(),
            DEFAULT_MAX_ROWS
        );
    }

    #[tokio::test]
    async fn a_tool_reads_the_budget_its_loop_set() {
        let set = ResultBudget::with_limits(7, 9_000);
        assert_eq!(with_budget(set, async { current_budget() }).await, set);
        assert_eq!(current_budget(), ResultBudget::default());
    }

    #[test]
    fn a_string_result_reaches_the_model_unescaped() {
        assert_eq!(
            tool_result_text(&Value::String("a \"quoted\"\nline".to_string())),
            "a \"quoted\"\nline"
        );
        assert_eq!(
            tool_result_text(&json!({"rows": [[1]]})),
            r#"{"rows":[[1]]}"#
        );
    }

    #[test]
    fn the_schema_describes_the_result() {
        let schema = result_schema();
        assert_eq!(schema["required"], json!(["columns", "rows"]));
    }
}
