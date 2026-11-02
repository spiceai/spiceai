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

//! Comparing a Spice answer with an expected one.
//!
//! The row comparison itself is `result-compare`'s, shared with testoperator and
//! the Cayenne correctness lanes. This module adds what a scenario needs around
//! it: typing an oracle's text answer, literal rows, and the rule that two empty
//! answers have compared nothing.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context, Result, bail, ensure};
use arrow::{
    array::{
        ArrayRef, BinaryArray, DurationNanosecondArray, LargeListArray, ListArray, RecordBatch,
        StringArray,
    },
    buffer::{NullBuffer, OffsetBuffer},
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    util::{
        display::{ArrayFormatter, FormatOptions},
        pretty::pretty_format_batches,
    },
};
use result_compare::{QueryValidationResult, validate_against_reference};
use serde_json::Value;

use crate::source::TextResult;

/// Types an oracle's text answer with the column types of Spice's answer, so the
/// two can be compared value by value.
///
/// Typing the oracle by Spice's schema means this comparison cannot catch a
/// wrong *type* — a column mapped to `Utf8` that should be `Int64` still
/// compares equal when the digits match. Type mapping needs its own check
/// against an expected mapping; this one is about values.
pub fn type_oracle(text: &TextResult, schema: &SchemaRef) -> Result<RecordBatch> {
    if text.rows.is_empty() {
        // Some oracles (MySQL's XML) name no columns in an empty answer.
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    if text.columns.len() != schema.fields().len() {
        bail!(
            "the oracle returned {} columns ({}) but Spice returned {} ({})",
            text.columns.len(),
            text.columns.join(", "),
            schema.fields().len(),
            schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let cells: Vec<Option<&str>> = text.rows.iter().map(|row| row[i].as_deref()).collect();
            type_cells(&cells, field.data_type()).with_context(|| {
                format!(
                    "Failed to type the oracle's column '{}' as {}",
                    text.columns[i],
                    field.data_type()
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Types one column's text cells as `data_type`.
fn type_cells(cells: &[Option<&str>], data_type: &DataType) -> Result<ArrayRef> {
    let strict = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    Ok(match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            let bytes = cells
                .iter()
                .map(|c| c.map(decode_bytea).transpose())
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(BinaryArray::from_iter(bytes));
            cast_with_options(&array, data_type, &strict)?
        }
        // A boolean oracle column (PostgreSQL's t/f) against Spice's integer
        // reading of MySQL's TINYINT(1) BOOLEAN.
        data_type
            if data_type.is_integer()
                && cells.iter().flatten().all(|c| matches!(*c, "t" | "f"))
                && cells.iter().flatten().next().is_some() =>
        {
            let array: ArrayRef = Arc::new(StringArray::from_iter(
                cells
                    .iter()
                    .map(|c| c.map(|v| if v == "t" { "1" } else { "0" })),
            ));
            cast_with_options(&array, data_type, &strict)?
        }
        // MySQL prints `BIT` as hex bytes, which Spice reads as an integer.
        data_type
            if data_type.is_integer() && cells.iter().flatten().any(|c| c.starts_with("0x")) =>
        {
            let decimal = cells
                .iter()
                .map(|c| {
                    c.map(|v| match v.strip_prefix("0x") {
                        Some(hex) => u64::from_str_radix(hex, 16)
                            .map(|n| n.to_string())
                            .with_context(|| format!("'{v}' is not a hex integer")),
                        None => Ok(v.to_string()),
                    })
                    .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(StringArray::from_iter(decimal));
            cast_with_options(&array, data_type, &strict)?
        }
        // PostgreSQL writes a timestamp difference as an interval
        // (`73049 days 23:59:59.999999`), where Spice plans a duration.
        DataType::Duration(_)
            if cells
                .iter()
                .flatten()
                .any(|c| c.contains(':') || c.contains("day")) =>
        {
            let nanos = cells
                .iter()
                .map(|c| c.map(pg_interval_nanos).transpose())
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(DurationNanosecondArray::from(nanos));
            cast_with_options(&array, data_type, &strict)?
        }
        // PostgreSQL prints a whole-hour UTC offset as `+05`, which Arrow only
        // parses as `+05:00`. Only a column typed as a time-zone timestamp is
        // read that way, so text of the same shape is left alone.
        DataType::Timestamp(_, Some(_)) => {
            let array: ArrayRef = Arc::new(StringArray::from_iter(
                cells.iter().map(|c| c.map(complete_utc_offset)),
            ));
            cast_with_options(&array, data_type, &strict)?
        }
        DataType::Boolean => {
            let array: ArrayRef = Arc::new(StringArray::from_iter(cells.iter().map(|c| {
                c.map(|v| match v {
                    "t" => "true",
                    "f" => "false",
                    other => other,
                })
            })));
            cast_with_options(&array, data_type, &strict)?
        }
        DataType::Dictionary(_, value_type) => {
            let values = type_cells(cells, value_type)?;
            cast_with_options(&values, data_type, &strict)?
        }
        DataType::List(element) | DataType::LargeList(element) => {
            let lists = cells
                .iter()
                .map(|c| c.map(parse_array_literal).transpose())
                .collect::<Result<Vec<_>>>()?;
            let elements: Vec<Option<&str>> = lists
                .iter()
                .flatten()
                .flat_map(|items| items.iter().map(Option::as_deref))
                .collect();
            let values = type_cells(&elements, element.data_type())?;
            let lengths = lists.iter().map(|l| l.as_ref().map_or(0, Vec::len));
            let nulls = NullBuffer::from_iter(lists.iter().map(Option::is_some));
            let field = Arc::clone(element);
            match data_type {
                DataType::List(_) => Arc::new(ListArray::try_new(
                    field,
                    OffsetBuffer::<i32>::from_lengths(lengths),
                    values,
                    Some(nulls),
                )?),
                _ => Arc::new(LargeListArray::try_new(
                    field,
                    OffsetBuffer::<i64>::from_lengths(lengths),
                    values,
                    Some(nulls),
                )?),
            }
        }
        data_type => {
            let array: ArrayRef = Arc::new(StringArray::from_iter(cells.iter().copied()));
            cast_with_options(&array, data_type, &strict)?
        }
    })
}

/// `+05` → `+05:00` at the end of a timestamp; anything else unchanged.
fn complete_utc_offset(text: &str) -> String {
    let bytes = text.as_bytes();
    let n = bytes.len();
    if n > 3
        && (bytes[n - 3] == b'+' || bytes[n - 3] == b'-')
        && bytes[n - 2].is_ascii_digit()
        && bytes[n - 1].is_ascii_digit()
        && text[..n - 3].contains(':')
    {
        return format!("{text}:00");
    }
    text.to_string()
}

/// Splits a one-dimensional array literal into its elements: Postgres's
/// `{1,NULL,"a,b"}` (an unquoted `NULL` is NULL, a quoted element may hold
/// commas, quotes and backslashes, escaped), `DuckDB`'s `[1, NULL, a]`, or
/// `ClickHouse`'s `[1,NULL,2]` / `['a',NULL,'b\'c']`.
/// Elements separated by `,`, a `quote`d one holding commas and escaped
/// quotes or backslashes, an unquoted `NULL` being NULL.
fn parse_quoted_list(inner: &str, quote: char) -> Result<Vec<Option<String>>> {
    let mut items = Vec::new();
    let mut chars = inner.chars().peekable();
    loop {
        if chars.peek() == Some(&quote) {
            chars.next();
            let mut value = String::new();
            loop {
                match chars.next() {
                    Some('\\') => {
                        value.push(chars.next().context("dangling escape in array literal")?);
                    }
                    Some(c) if c == quote => break,
                    Some(c) => value.push(c),
                    None => bail!("unterminated quoted element in '[{inner}]'"),
                }
            }
            items.push(Some(value));
        } else {
            let mut value = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                value.push(c);
                chars.next();
            }
            items.push((value != "NULL").then_some(value));
        }
        match chars.next() {
            None => return Ok(items),
            Some(',') => {}
            Some(c) => bail!("expected ',' after an element of '[{inner}]', got '{c}'"),
        }
    }
}

fn parse_array_literal(text: &str) -> Result<Vec<Option<String>>> {
    let text = text.trim();
    if let Some(inner) = text.strip_prefix('[').and_then(|t| t.strip_suffix(']')) {
        if inner.trim().is_empty() {
            return Ok(Vec::new());
        }
        // ClickHouse quotes strings and separates with a bare comma; DuckDB
        // separates with ", " and quotes nothing.
        if inner.starts_with('\'') || !inner.contains(", ") {
            return parse_quoted_list(inner, '\'');
        }
        return Ok(inner
            .split(", ")
            .map(|e| (e != "NULL").then(|| e.to_string()))
            .collect());
    }
    let inner = text
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .with_context(|| format!("expected an array literal, got '{text}'"))?;
    let mut items = Vec::new();
    if inner.is_empty() {
        return Ok(items);
    }
    let mut chars = inner.chars().peekable();
    loop {
        let item = if chars.peek() == Some(&'"') {
            chars.next();
            let mut value = String::new();
            loop {
                match chars.next() {
                    Some('\\') => {
                        value.push(chars.next().context("dangling escape in array literal")?);
                    }
                    Some('"') => break,
                    Some(c) => value.push(c),
                    None => bail!("unterminated quoted element in '{text}'"),
                }
            }
            Some(value)
        } else {
            let mut value = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                ensure!(c != '{', "nested arrays are not supported: '{text}'");
                value.push(c);
                chars.next();
            }
            (!value.eq_ignore_ascii_case("NULL")).then_some(value)
        };
        items.push(item);
        match chars.next() {
            Some(',') => {}
            None => break,
            Some(c) => bail!("unexpected '{c}' in array literal '{text}'"),
        }
    }
    Ok(items)
}

/// A PostgreSQL interval of days and a time (`-1 days +23:00:00.5`, `3 days`,
/// `00:00:01`) in nanoseconds. Months and years have no fixed length, so an
/// interval with them is refused rather than guessed.
fn pg_interval_nanos(text: &str) -> Result<i64> {
    const NANOS_PER_SECOND: i128 = 1_000_000_000;
    let mut total: i128 = 0;
    let mut words = text.split_whitespace().peekable();
    while let Some(word) = words.next() {
        if word.contains(':') {
            let (sign, clock) = match word.strip_prefix('-') {
                Some(rest) => (-1, rest),
                None => (1, word.trim_start_matches('+')),
            };
            let mut parts = clock.splitn(3, ':');
            let (Some(h), Some(m), Some(sec)) = (parts.next(), parts.next(), parts.next()) else {
                bail!("expected hh:mm:ss in the interval '{text}'");
            };
            let (whole, fraction) = sec.split_once('.').unwrap_or((sec, ""));
            ensure!(
                fraction.len() <= 9,
                "more than nanosecond precision in '{text}'"
            );
            let fraction_nanos: i128 = if fraction.is_empty() {
                0
            } else {
                format!("{fraction:0<9}").parse()?
            };
            let seconds =
                h.parse::<i128>()? * 3600 + m.parse::<i128>()? * 60 + whole.parse::<i128>()?;
            total += sign * (seconds * NANOS_PER_SECOND + fraction_nanos);
        } else {
            let count: i128 = word
                .parse()
                .with_context(|| format!("unexpected '{word}' in the interval '{text}'"))?;
            let unit = words
                .next()
                .with_context(|| format!("a count without a unit in '{text}'"))?;
            ensure!(
                unit.starts_with("day"),
                "the interval '{text}' has {unit}, which has no fixed length"
            );
            total += count * 86_400 * NANOS_PER_SECOND;
        }
    }
    Ok(i64::try_from(total)?)
}

/// Bytes printed as hex: Postgres's `\x…` or MySQL's `0x…`.
fn decode_bytea(text: &str) -> Result<Vec<u8>> {
    let hex_digits = text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("0x"))
        .with_context(|| format!("expected hex bytes, got '{text}'"))?;
    Ok(hex::decode(hex_digits)?)
}

/// A literal expected answer, typed like Spice's answer.
pub fn literal_rows(rows: &[Vec<Value>], schema: &SchemaRef) -> Result<RecordBatch> {
    let text = TextResult {
        columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
        rows: rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| match v {
                        Value::Null => None,
                        Value::String(s) => Some(s.clone()),
                        other => Some(other.to_string()),
                    })
                    .collect()
            })
            .collect(),
    };
    if let Some(bad) = text.rows.iter().find(|r| r.len() != text.columns.len()) {
        bail!(
            "an expected row has {} cells but Spice returned {} columns",
            bad.len(),
            text.columns.len()
        );
    }
    type_oracle(&text, schema)
}

/// Compares Spice's answer with the expected one. `Ok(None)` is a match; the
/// string is a report of the mismatch.
pub fn compare(
    name: &str,
    sql: &str,
    actual: &[RecordBatch],
    expected: &[RecordBatch],
    expect_empty: bool,
) -> Result<Option<String>> {
    let actual_rows: usize = actual.iter().map(RecordBatch::num_rows).sum();
    let expected_rows: usize = expected.iter().map(RecordBatch::num_rows).sum();
    if actual_rows == 0 && expected_rows == 0 {
        return Ok((!expect_empty).then(|| {
            "both answers are empty, which compares nothing; set `expect-empty: true` if an empty answer is the point".to_string()
        }));
    }
    if expect_empty {
        return Ok(Some(format!(
            "expected an empty answer, got {actual_rows} rows from Spice and {expected_rows} expected"
        )));
    }
    // The comparator names its first side `expected` in a mismatch report and
    // checks the sort order of both sides alike, so the expected answer goes
    // first for the report to read the right way round.
    match validate_against_reference(name, sql, expected, actual)? {
        QueryValidationResult::Pass => Ok(None),
        QueryValidationResult::Fail(reason) => Ok(Some(format!(
            "{reason:?}\n--- spice ({actual_rows} rows)\n{}\n--- expected ({expected_rows} rows)\n{}",
            preview(actual),
            preview(expected)
        ))),
    }
}

/// The widest a value is drawn in a failure report's table, in characters,
/// unless `--cell-width` says otherwise. Mismatches show in the first
/// characters or the last; the values that run longer are the large ones a
/// script writes on purpose, and drawn whole they make each line of the
/// table as wide as they are.
pub const DEFAULT_CELL_WIDTH: usize = 40;

static CELL_WIDTH: AtomicUsize = AtomicUsize::new(DEFAULT_CELL_WIDTH);

/// Sets the widest a value is drawn in a failure report's table; 0 draws
/// every value whole.
pub fn set_cell_width(width: usize) {
    CELL_WIDTH.store(width, Ordering::Relaxed);
}

/// `value`, or its start and end around `…` when it is longer than `width`
/// characters.
fn shortened(value: String, width: usize) -> String {
    let length = value.chars().count();
    if width == 0 || length <= width {
        return value;
    }
    let tail = (width - 1) / 2;
    let head = width - 1 - tail;
    let mut short: String = value.chars().take(head).collect();
    short.push('…');
    short.extend(value.chars().skip(length - tail));
    short
}

/// `batch` with each value as the text a table draws, shortened to `width`.
fn shortened_batch(batch: &RecordBatch, width: usize) -> Result<RecordBatch, ArrowError> {
    let options = FormatOptions::default().with_display_error(true);
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let formatter = ArrayFormatter::try_new(column.as_ref(), &options)?;
            let values: StringArray = (0..column.len())
                .map(|i| {
                    column
                        .is_valid(i)
                        .then(|| shortened(formatter.value(i).to_string(), width))
                })
                .collect();
            Ok(Arc::new(values) as ArrayRef)
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), DataType::Utf8, true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

/// Protocols serialize the same result types, so equality is exact after
/// normalizing row order and SQL's equivalent representations of zero.
/// The ordinary comparator additionally checks explicit SQL sort contracts.
pub fn compare_protocol(
    name: &str,
    sql: &str,
    actual: &[RecordBatch],
    expected: &[RecordBatch],
    expect_empty: bool,
) -> Result<Option<String>> {
    if let Some(report) = compare(name, sql, actual, expected, expect_empty)? {
        return Ok(Some(report));
    }
    let preserve_order = actual
        .first()
        .or_else(|| expected.first())
        .is_some_and(|batch| {
            result_compare::sort_order::resolve_sort_key(sql, &batch.schema()).hides_a_sort_term()
        });
    let rows = |batches: &[RecordBatch]| -> Result<Vec<Vec<Option<String>>>> {
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| result_compare::array_value_to_string(column.as_ref(), row))
                        .collect::<Result<Vec<_>>>()?,
                );
            }
        }
        if !preserve_order {
            rows.sort_unstable();
        }
        Ok(rows)
    };
    let actual_rows = rows(actual)?;
    let expected_rows = rows(expected)?;
    if actual_rows != expected_rows {
        return Ok(Some(format!(
            "protocol values differ (exact comparison); first different rows: {:?}\n--- actual\n{}\n--- expected\n{}",
            actual_rows
                .iter()
                .zip(&expected_rows)
                .find(|(actual, expected)| actual != expected),
            preview(actual),
            preview(expected)
        )));
    }
    Ok(None)
}

/// The first rows of an answer, for a failure report.
pub fn preview(batches: &[RecordBatch]) -> String {
    const ROWS: usize = 10;
    let mut left = ROWS;
    let head: Vec<RecordBatch> = batches
        .iter()
        .filter_map(|b| {
            let take = left.min(b.num_rows());
            left -= take;
            (take > 0).then(|| b.slice(0, take))
        })
        .collect();
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let width = CELL_WIDTH.load(Ordering::Relaxed);
    let table = head
        .iter()
        .map(|b| shortened_batch(b, width))
        .collect::<Result<Vec<_>, _>>()
        .and_then(|head| pretty_format_batches(&head))
        .map_or_else(|e| e.to_string(), |t| t.to_string());
    if total > ROWS {
        format!("{table}\n… {} more rows", total - ROWS)
    } else {
        table
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, Decimal128Array, Int64Array, TimestampMicrosecondArray},
        datatypes::TimeUnit,
    };

    use result_compare::array_value_to_string;

    use super::*;

    #[test]
    fn protocol_values_use_exact_numeric_comparison() {
        use arrow::array::Float64Array;
        let batch = |values: Vec<f64>| {
            RecordBatch::try_from_iter([("f", Arc::new(Float64Array::from(values)) as ArrayRef)])
                .expect("float batch")
        };
        let left = vec![batch(vec![1.0, 1.000_001])];
        let changed = vec![batch(vec![1.0, 1.000_002])];
        assert!(
            compare_protocol("protocols", "SELECT f FROM t", &left, &changed, false)
                .expect("comparison")
                .is_some()
        );
        let swapped = vec![batch(vec![1.000_001, 1.0])];
        assert!(
            compare_protocol("protocols", "SELECT f FROM t", &left, &swapped, false)
                .expect("unordered comparison")
                .is_none()
        );
        assert!(
            compare_protocol(
                "protocols",
                "SELECT f FROM t ORDER BY hidden_key",
                &left,
                &swapped,
                false
            )
            .expect("hidden order comparison")
            .is_some()
        );
    }

    fn text(rows: &[&[Option<&str>]]) -> TextResult {
        TextResult {
            columns: (0..rows[0].len()).map(|i| format!("c{i}")).collect(),
            rows: rows
                .iter()
                .map(|r| r.iter().map(|c| c.map(str::to_string)).collect())
                .collect(),
        }
    }

    #[test]
    fn completes_a_utc_offset_only_where_a_time_zone_timestamp_is_read() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let batch = type_oracle(
            &text(&[
                &[Some("ticket:batch-05"), Some("2024-01-01 00:00:00+05")],
                &[Some("10:00-05"), Some("2024-01-01 00:00:00+05:30")],
            ]),
            &schema,
        )
        .expect("typed");
        let rendered: Vec<Vec<String>> = (0..batch.num_rows())
            .map(|row| {
                (0..2)
                    .map(|col| {
                        array_value_to_string(batch.column(col), row)
                            .expect("rendered")
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                vec![
                    "ticket:batch-05".to_string(),
                    "2023-12-31 19:00:00.000000".to_string()
                ],
                vec![
                    "10:00-05".to_string(),
                    "2023-12-31 18:30:00.000000".to_string()
                ],
            ]
        );
    }

    #[test]
    fn types_the_oracle_by_spices_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bal", DataType::Decimal128(15, 2), true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("ok", DataType::Boolean, true),
            Field::new("b", DataType::Binary, true),
        ]));
        let batch = type_oracle(
            &text(&[
                &[
                    Some("1"),
                    Some("8324.07"),
                    Some("2024-03-15 10:00:00+00:00"),
                    Some("t"),
                    Some("\\x0aff"),
                ],
                &[Some("2"), None, None, Some("f"), None],
            ]),
            &schema,
        )
        .expect("types");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(ids.values(), &[1, 2]);
        let bal = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal");
        assert_eq!(bal.value(0), 832_407);
        assert!(bal.is_null(1));
        let ts = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        assert_eq!(ts.value(0), 1_710_496_800_000_000);
        let render = |column: usize, row: usize| {
            array_value_to_string(batch.column(column).as_ref(), row).expect("renders")
        };
        assert_eq!(render(3, 1).as_deref(), Some("false"));
        assert_eq!(render(4, 0).as_deref(), Some("\\x0aff"));
    }

    #[test]
    fn an_oracle_value_that_does_not_fit_the_type_is_an_error_not_a_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)]));
        let err = type_oracle(&text(&[&[Some("99999999999")]]), &schema).expect_err("overflow");
        assert!(format!("{err:#}").contains("column 'c0'"), "{err:#}");
    }

    #[test]
    fn two_empty_answers_fail_unless_declared() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let empty = RecordBatch::new_empty(schema);
        let both = std::slice::from_ref(&empty);
        let report = compare("q", "SELECT n FROM t", both, both, false).expect("compares");
        assert!(report.is_some_and(|r| r.contains("compares nothing")));
        assert_eq!(
            compare("q", "SELECT n FROM t", both, both, true).expect("compares"),
            None
        );
    }

    #[test]
    fn types_array_literals_as_lists_with_null_elements_and_quoting() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ints",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int16, true))),
                true,
            ),
            Field::new(
                "texts",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
        ]));
        let batch = type_oracle(
            &text(&[
                &[
                    Some("{1,2}"),
                    Some(r#"{plain,"a,b","say \"hi\"",NULL,"NULL"}"#),
                ],
                &[None, Some("{}")],
                &[Some("[10, NULL, 30]"), Some("[x, y]")],
                &[Some("[-32768,32767]"), Some(r"['a',NULL,'','it\'s, ok']")],
                &[Some("[7]"), Some("['日本語']")],
            ]),
            &schema,
        )
        .expect("typed");
        let rendered: Vec<Vec<Option<String>>> = (0..batch.num_rows())
            .map(|row| {
                (0..batch.num_columns())
                    .map(|col| array_value_to_string(batch.column(col), row).expect("rendered"))
                    .collect()
            })
            .collect();
        let s = |v: &str| Some(v.to_string());
        assert_eq!(
            rendered,
            vec![
                vec![
                    s(r#"["1", "2"]"#),
                    s(r#"["plain", "a,b", "say \"hi\"", NULL, "NULL"]"#)
                ],
                vec![None, s("[]")],
                vec![s(r#"["10", NULL, "30"]"#), s(r#"["x", "y"]"#)],
                vec![
                    s(r#"["-32768", "32767"]"#),
                    s(r#"["a", NULL, "", "it's, ok"]"#)
                ],
                vec![s(r#"["7"]"#), s(r#"["日本語"]"#)],
            ]
        );
        let texts = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let first = texts.value(0);
        assert!(first.is_null(3), "an unquoted NULL is a NULL element");
        assert!(!first.is_null(4), "a quoted \"NULL\" is text");
        assert!(batch.column(0).is_null(1), "a NULL cell is a NULL list");
    }

    #[test]
    fn a_long_value_is_drawn_by_its_start_and_end() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("s", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("short".to_string()),
                    Some(format!("start{}é-end", "x".repeat(1000))),
                    None,
                ])),
            ],
        )
        .expect("batch");
        let draw = |width| {
            pretty_format_batches(&[shortened_batch(&batch, width).expect("shortened")])
                .expect("table")
                .to_string()
        };
        assert_eq!(
            draw(12),
            "+----+--------------+\n\
             | id | s            |\n\
             +----+--------------+\n\
             | 1  | short        |\n\
             | 2  | startx…é-end |\n\
             | 3  |              |\n\
             +----+--------------+"
        );
        // 0 draws each value whole, as the table did before.
        assert_eq!(
            draw(0),
            pretty_format_batches(std::slice::from_ref(&batch))
                .expect("table")
                .to_string()
        );
    }
}
