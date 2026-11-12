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
    columns: Vec<String>,
    /// A JSON array of row arrays.
    rows: Vec<u8>,
    shown: usize,
    truncated: bool,
    shortened: bool,
    max_bytes: usize,
}

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

    /// The message for the model, when rows were left out or values shortened.
    #[must_use]
    pub fn message(&self) -> Option<String> {
        let rows = if !self.truncated {
            None
        } else if self.shown == 0 {
            Some(format!(
                "The first row is larger than the {} KiB this result may use, so no rows are shown. Select fewer columns, or select long columns on their own.",
                self.max_bytes / 1024
            ))
        } else {
            Some(format!(
                "Showing the first {} rows; the result has more. Do not compute totals, counts, or averages from these rows. Run an aggregate query instead, or narrow the query with a WHERE clause.",
                self.shown
            ))
        };
        let values = self.shortened.then_some(
            "Some long values were shortened; select the column on its own to read it in full.",
        );
        match (rows, values) {
            (None, None) => None,
            (Some(rows), None) => Some(rows),
            (None, Some(values)) => Some(values.to_string()),
            (Some(rows), Some(values)) => Some(format!("{rows} {values}")),
        }
    }

    /// The tool result: `{"columns":[…],"rows":[…]}`, with `truncated` when rows were left out and
    /// `message` when rows were left out or values shortened.
    ///
    /// # Errors
    ///
    /// Returns an error if the encoded rows are not valid JSON, which would be a bug in the
    /// encoder.
    pub fn into_value(self) -> Result<Value, serde_json::Error> {
        let message = self.message();
        let mut object = Map::with_capacity(4);
        object.insert(
            "columns".to_string(),
            Value::Array(self.columns.into_iter().map(Value::String).collect()),
        );
        object.insert("rows".to_string(), serde_json::from_slice(&self.rows)?);
        if self.truncated {
            object.insert("truncated".to_string(), Value::Bool(true));
        }
        if let Some(message) = message {
            object.insert("message".to_string(), Value::String(message));
        }
        Ok(Value::Object(object))
    }
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
    budget: ResultBudget,
    /// The bytes the encoded rows may take: the budget less the column names and the envelope.
    rows_budget: usize,
    rows: Vec<u8>,
    row: Vec<u8>,
    shown: usize,
    truncated: bool,
    shortened: Arc<AtomicBool>,
    options: EncoderOptions,
}

impl RowEncoder {
    #[must_use]
    pub fn new(schema: &SchemaRef, budget: ResultBudget) -> Self {
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let columns_bytes = columns.iter().map(|name| name.len() + 3).sum::<usize>() + 2;
        let rows_budget = budget
            .max_bytes
            .saturating_sub(columns_bytes.saturating_add(ENVELOPE_BYTES));
        let shortened = Arc::new(AtomicBool::new(false));
        let options = EncoderOptions::default()
            .with_explicit_nulls(true)
            .with_encoder_factory(Arc::new(ModelValueEncoders {
                shortened: Arc::clone(&shortened),
            }));
        let mut rows = Vec::with_capacity(rows_budget.min(MAX_RESULT_BYTES));
        rows.push(b'[');
        Self {
            columns,
            budget,
            rows_budget,
            rows,
            row: Vec::new(),
            shown: 0,
            truncated: false,
            shortened,
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
            let shortened_before = self.shortened.load(Ordering::Relaxed);
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
                self.shortened.store(shortened_before, Ordering::Relaxed);
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
        ToolRows {
            columns: self.columns,
            rows: self.rows,
            shown: self.shown,
            truncated: self.truncated,
            shortened: self.shortened.load(Ordering::Relaxed),
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
    /// Set when a value is shortened.
    shortened: Arc<AtomicBool>,
}

impl EncoderFactory for ModelValueEncoders {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let shortened = || Arc::clone(&self.shortened);
        let encoder: Box<dyn Encoder + 'a> = match array.data_type() {
            DataType::Utf8 => Box::new(TextEncoder {
                values: array.as_string::<i32>(),
                shortened: shortened(),
            }),
            DataType::LargeUtf8 => Box::new(TextEncoder {
                values: array.as_string::<i64>(),
                shortened: shortened(),
            }),
            DataType::Utf8View => Box::new(TextEncoder {
                values: array.as_string_view(),
                shortened: shortened(),
            }),
            DataType::Binary => Box::new(BinaryEncoder {
                values: array.as_binary::<i32>(),
            }),
            DataType::LargeBinary => Box::new(BinaryEncoder {
                values: array.as_binary::<i64>(),
            }),
            DataType::BinaryView => Box::new(BinaryEncoder {
                values: array.as_binary_view(),
            }),
            DataType::FixedSizeBinary(_) => Box::new(BinaryEncoder {
                values: array.as_fixed_size_binary(),
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
            DataType::List(item) => {
                let list = array.as_list::<i32>();
                Box::new(ListEncoder {
                    ranges: ListRanges::Offsets(
                        list.value_offsets()
                            .iter()
                            .map(|offset| offset_to_usize(*offset))
                            .collect::<Result<_, _>>()?,
                    ),
                    values: make_encoder(item, list.values().as_ref(), options)?,
                    shortened: shortened(),
                })
            }
            DataType::LargeList(item) => {
                let list = array.as_list::<i64>();
                Box::new(ListEncoder {
                    ranges: ListRanges::Offsets(
                        list.value_offsets()
                            .iter()
                            .map(|offset| offset_to_usize(*offset))
                            .collect::<Result<_, _>>()?,
                    ),
                    values: make_encoder(item, list.values().as_ref(), options)?,
                    shortened: shortened(),
                })
            }
            DataType::FixedSizeList(item, size) => {
                let list = array.as_fixed_size_list();
                Box::new(ListEncoder {
                    ranges: ListRanges::Fixed(offset_to_usize(*size)?),
                    values: make_encoder(item, list.values().as_ref(), options)?,
                    shortened: shortened(),
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
    shortened: Arc<AtomicBool>,
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
                self.shortened.store(true, Ordering::Relaxed);
                write_json_string(&format!("{}…[+{left_out} chars]", &value[..cut]), out);
            }
        }
    }
}

/// A binary value, as its length.
struct BinaryEncoder<A> {
    values: A,
}

impl<'a, A> Encoder for BinaryEncoder<A>
where
    A: ArrayAccessor<Item = &'a [u8]>,
{
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let len = self.values.value(idx).len();
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

/// A value written as a JSON string of its display form: a decimal too wide for an `f64`.
struct QuotedDisplayEncoder<'a> {
    formatter: ArrayFormatter<'a>,
}

impl Encoder for QuotedDisplayEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        write_json_string(&self.formatter.value(idx).to_string(), out);
    }
}

enum ListRanges {
    /// Where each list starts and ends in the values: list `i` is `offsets[i]..offsets[i + 1]`.
    Offsets(Vec<usize>),
    /// Every list has this many values.
    Fixed(usize),
}

impl ListRanges {
    fn range(&self, idx: usize) -> (usize, usize) {
        match self {
            ListRanges::Offsets(offsets) => (
                offsets.get(idx).copied().unwrap_or_default(),
                offsets.get(idx + 1).copied().unwrap_or_default(),
            ),
            ListRanges::Fixed(size) => (idx * size, (idx + 1) * size),
        }
    }
}

/// A list, shortened to [`MAX_LIST_ITEMS`] elements with a marker of how many were left out.
struct ListEncoder<'a> {
    ranges: ListRanges,
    values: NullableEncoder<'a>,
    shortened: Arc<AtomicBool>,
}

impl Encoder for ListEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let (start, end) = self.ranges.range(idx);
        let len = end.saturating_sub(start);
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
            self.shortened.store(true, Ordering::Relaxed);
            if shown > 0 {
                out.push(b',');
            }
            write_json_string(&format!("…[+{} items]", len - shown), out);
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
            Int64Array, ListArray, StringArray, StructArray,
        },
        datatypes::{Field, Int32Type},
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
                "message": "The first row is larger than the 4 KiB this result may use, so no rows are shown. Select fewer columns, or select long columns on their own.",
            })
        );
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
        let value = encode_batches(
            &batch(vec![("t", Arc::new(text.clone()) as ArrayRef)]).schema(),
            &[batch(vec![
                ("t", Arc::new(text) as ArrayRef),
                ("l", Arc::new(list) as ArrayRef),
                ("b", Arc::new(bytes) as ArrayRef),
            ])],
            ResultBudget::default(),
        )
        .expect("encodes")
        .into_value()
        .expect("valid JSON");
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
            Value::String(
                "Some long values were shortened; select the column on its own to read it in full."
                    .to_string()
            )
        );
        assert!(value.get("truncated").is_none());
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
