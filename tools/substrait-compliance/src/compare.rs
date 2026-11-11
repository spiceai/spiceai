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

//! Result comparison for Mode A TPC-H.
//!
//! Base rules follow the IBM TPC-H suite README (`test-suites/tpch/README.md`
//! at tag v0.1.1): row count, column count, normalised types, then per-cell
//! values. The IBM Rust SDK comparator does not check column names at all.
//!
//! Harness lifts (known-fail cosmetics only — values must still match):
//! - `integer` and `bigint` are type-compatible (`COUNT` is `Int64` in
//!   `DataFusion`; `DuckDB` goldens label it `integer`).
//! - Column names are not compared (plan alias `TOTAL_VALUE` vs `DuckDB`
//!   `value`; Isthmus `L_RETURNFLAG` vs golden `l_returnflag`).
//! - String / `CHAR` cells compare after trailing-pad trim only
//!   (leading spaces are significant — q02 `s_comment`).
//! - Quoted-empty `""` in a golden CSV cell decodes to empty/NULL
//!   (IBM TPC-H README). After decode, only the empty string is NULL;
//!   a literal `""` (two quote characters) is a nonempty string.
//! - Numerics: `integer`/`bigint` cells compare exactly. Floats/`double`
//!   match when `|Δ| < 1e-8` (q06) or relative error is `< 1e-14` (q01
//!   `sum_charge` `DataFusion`/`DuckDB` conversion). Printed fractional
//!   length is not a tolerance — a 2-digit actual like `0.06` must not
//!   match `0.05008…`. Two declared `decimal(p,s)` / `numeric(p,s)` cells
//!   compare exactly as scaled integers (never through `f64`, which
//!   collapses values past 15 significant digits), allowing one ULP at
//!   the shared scale when it is ≥ 2. When the engine's own schema declares the actual
//!   column `decimal(p,s)` and the golden is `double`, the actual must be
//!   the golden rounded or truncated at that scale (q01 `AVG_QTY`
//!   `decimal(19,6)` `25.575154` vs `25.575154611454693`); a value one unit
//!   off still fails. IBM README is absolute `1e-9` only.
//!
//! Not lifted: row-count misses. `string` ↔ numeric type labels
//! (q22 country codes) are compatible; `values_match` decides PASS/FAIL.

/// Absolute numeric floor. IBM documents `1e-9`; `1e-8` covers q06's
/// `DuckDB`-vs-`DataFusion` rounding (Δ ≈ 1.16e-9).
const NUMERIC_ABS_EPSILON: f64 = 1e-8;

/// Minimum declared decimal scale treated as a ULP tolerance. Scale 1 is
/// float formatting (`.0`, `.1`), not `decimal(p, 1)`.
const MIN_DECIMAL_SCALE: i32 = 2;

/// Relative cap for large `DECIMAL` vs float64 conversion (q01 `sum_charge`
/// `|Δ| ≈ 1.4e-6` on ~`5.3e8` is ~`3e-15`). `1e-14` of a `1e9` magnitude
/// is `1e-5`, so an off-by-one `COUNT` and a `0.50` money error still miss.
const NUMERIC_REL_EPSILON: f64 = 1e-14;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub type_token: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableData {
    pub columns: Vec<ColumnSpec>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompareMismatch {
    RowCount {
        actual: usize,
        expected: usize,
    },
    ColumnCount {
        actual: usize,
        expected: usize,
    },
    ColumnType {
        index: usize,
        actual: String,
        expected: String,
    },
    RowWidth {
        row: usize,
        actual: usize,
        expected: usize,
        columns: usize,
    },
    Value {
        row: usize,
        column: usize,
        actual: String,
        expected: String,
    },
    /// Expected table has no typed columns. A zero-byte or headerless
    /// golden must not compare equal to an empty execution result.
    MissingTypedHeader,
}

impl std::fmt::Display for CompareMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RowCount { actual, expected } => {
                write!(f, "row count {actual} != {expected}")
            }
            Self::ColumnCount { actual, expected } => {
                write!(f, "column count {actual} != {expected}")
            }
            Self::ColumnType {
                index,
                actual,
                expected,
            } => write!(f, "column {index} type '{actual}' != '{expected}'"),
            Self::RowWidth {
                row,
                actual,
                expected,
                columns,
            } => write!(
                f,
                "row {row} field count actual={actual} expected={expected} schema={columns}"
            ),
            Self::Value {
                row,
                column,
                actual,
                expected,
            } => write!(f, "cell ({row},{column}) '{actual}' != '{expected}'"),
            Self::MissingTypedHeader => write!(
                f,
                "expected output has no typed columns (zero-byte or headerless golden)"
            ),
        }
    }
}

#[must_use]
pub fn compare(actual: &TableData, expected: &TableData) -> Option<CompareMismatch> {
    if expected.columns.is_empty() {
        return Some(CompareMismatch::MissingTypedHeader);
    }
    if actual.rows.len() != expected.rows.len() {
        return Some(CompareMismatch::RowCount {
            actual: actual.rows.len(),
            expected: expected.rows.len(),
        });
    }
    if actual.columns.len() != expected.columns.len() {
        return Some(CompareMismatch::ColumnCount {
            actual: actual.columns.len(),
            expected: expected.columns.len(),
        });
    }
    for (index, (a_col, e_col)) in actual
        .columns
        .iter()
        .zip(expected.columns.iter())
        .enumerate()
    {
        // Names are not compared: IBM Rust SDK skips them; Isthmus aliases
        // and letter-case differ from `DuckDB` goldens while values still match.
        if !types_compatible(&a_col.type_token, &e_col.type_token) {
            return Some(CompareMismatch::ColumnType {
                index,
                actual: a_col.type_token.clone(),
                expected: e_col.type_token.clone(),
            });
        }
    }
    let columns = expected.columns.len();
    for (row_idx, (a_row, e_row)) in actual.rows.iter().zip(expected.rows.iter()).enumerate() {
        if a_row.len() != columns || e_row.len() != columns {
            return Some(CompareMismatch::RowWidth {
                row: row_idx,
                actual: a_row.len(),
                expected: e_row.len(),
                columns,
            });
        }
        for (col_idx, (a_val, e_val)) in a_row.iter().zip(e_row.iter()).enumerate() {
            let actual_type = actual.columns.get(col_idx).map(|c| c.type_token.as_str());
            let expected_type = expected.columns.get(col_idx).map(|c| c.type_token.as_str());
            if !cells_match(a_val, e_val, actual_type, expected_type) {
                return Some(CompareMismatch::Value {
                    row: row_idx,
                    column: col_idx,
                    actual: a_val.clone(),
                    expected: e_val.clone(),
                });
            }
        }
    }
    None
}

#[must_use]
pub fn normalize_type(token: &str) -> &'static str {
    let trimmed = token.trim();
    let base_end = trimmed.find('(').unwrap_or(trimmed.len());
    let base = trimmed[..base_end].trim().to_ascii_lowercase();
    match base.as_str() {
        "integer" | "int" | "int32" | "i32" | "i8" | "i16" | "int4" | "smallint" | "tinyint" => {
            "integer"
        }
        "bigint" | "long" | "int64" | "i64" | "int8" => "bigint",
        "double" | "fp64" | "float8" | "numeric" | "decimal" | "number" => "double",
        "float" | "fp32" | "real" | "float4" => "float",
        "boolean" | "bool" => "boolean",
        // date/timestamp tokens compare as opaque strings (IBM TPC-H README).
        _ => "string",
    }
}

/// `integer` and `bigint` are the same TPC-H `COUNT` width under two labels.
/// IBM v0.1.1 also requires numeric/string cross-type values to reach
/// value compare (q22 country codes: engine `string` vs golden `integer`).
#[must_use]
pub fn types_compatible(actual: &str, expected: &str) -> bool {
    let a = normalize_type(actual);
    let e = normalize_type(expected);
    a == e
        || matches!((a, e), ("integer", "bigint") | ("bigint", "integer"))
        || string_numeric_pair(a, e)
}

fn is_numeric_kind(kind: &str) -> bool {
    matches!(kind, "integer" | "bigint" | "double" | "float")
}

fn string_numeric_pair(actual: &str, expected: &str) -> bool {
    (actual == "string" && is_numeric_kind(expected))
        || (expected == "string" && is_numeric_kind(actual))
}

#[must_use]
#[cfg_attr(not(test), expect(dead_code, reason = "used by unit tests"))]
pub fn values_match(actual: &str, expected: &str) -> bool {
    cells_match(actual, expected, None, None)
}

fn cells_match(
    actual: &str,
    expected: &str,
    actual_type: Option<&str>,
    expected_type: Option<&str>,
) -> bool {
    // When exactly one side is `string`, the numeric side decides, so the
    // documented numeric/string compatibility (q22 country codes) holds in
    // both directions; otherwise the golden's type, then the engine's.
    let actual_kind = actual_type.map(normalize_type);
    let expected_kind = expected_type.map(normalize_type);
    let kind = match (actual_kind, expected_kind) {
        (Some("string"), Some(expected)) if is_numeric_kind(expected) => Some(expected),
        (Some(actual), Some("string")) if is_numeric_kind(actual) => Some(actual),
        _ => expected_kind.or(actual_kind),
    };
    // Identical text is a match only for kinds compared as text: a numeric
    // column must parse, so a malformed value on both sides (`not-a-number`)
    // is not certified.
    let numeric = kind.is_some_and(is_numeric_kind);
    if actual == expected && !numeric {
        return true;
    }
    if is_null_cell(actual) && is_null_cell(expected) {
        return true;
    }
    if matches!(kind, Some("integer" | "bigint")) {
        return integers_equal(actual, expected);
    }
    if kind == Some("string") {
        return trim_trailing_char_pad(actual) == trim_trailing_char_pad(expected);
    }

    if integers_equal(actual, expected) {
        return true;
    }
    if let (Some(actual_type), Some(expected_type)) = (actual_type, expected_type)
        && declared_decimal_scale(actual_type).is_some()
        && declared_decimal_scale(expected_type).is_some()
        && let Some(equal) = decimals_match(
            actual,
            expected,
            shared_decimal_scale(Some(actual_type), Some(expected_type)),
        )
    {
        return equal;
    }
    if let (Ok(a), Ok(e)) = (actual.parse::<f64>(), expected.parse::<f64>()) {
        // Equal parses (including equal infinities) and NaN on both sides.
        if a.total_cmp(&e).is_eq() || (a.is_nan() && e.is_nan()) {
            return true;
        }
        return numerics_close(a, e, shared_decimal_scale(actual_type, expected_type))
            || rendered_at_declared_scale(a, e, engine_declared_scale(actual_type, expected_type));
    }
    if numeric {
        // A `double`/`float` column whose text does not parse as a number.
        return false;
    }
    let a_lower = actual.to_ascii_lowercase();
    let e_lower = expected.to_ascii_lowercase();
    if matches!(a_lower.as_str(), "true" | "false") && matches!(e_lower.as_str(), "true" | "false")
    {
        return a_lower == e_lower;
    }
    // Isthmus `CHAR` / fixed-char padding is trailing-only (q15 `s_address`).
    trim_trailing_char_pad(actual) == trim_trailing_char_pad(expected)
}

/// `CHAR` pad and `DuckDB` loader whitespace sit on the right. Leading
/// spaces are significant (q02 `s_comment` `' foxes boost'`).
fn trim_trailing_char_pad(value: &str) -> &str {
    value.trim_end()
}

/// Only the empty decoded string is NULL/empty: the golden's `""` and an
/// engine NULL both decode to it. Whitespace is a value — a one-space actual
/// must not pass against a NULL golden on an integer column; on a string
/// column the trailing-pad rule decides (`" "` vs `""` still match, q15).
fn is_null_cell(value: &str) -> bool {
    value.is_empty()
}

fn parse_integer(value: &str) -> Option<i128> {
    let value = value.trim();
    if let Ok(n) = value.parse::<i128>() {
        return Some(n);
    }
    let (whole, frac) = value.split_once('.')?;
    if frac.is_empty() || !frac.bytes().all(|b| b == b'0') {
        return None;
    }
    whole.parse().ok()
}

fn integers_equal(actual: &str, expected: &str) -> bool {
    match (parse_integer(actual), parse_integer(expected)) {
        (Some(a), Some(e)) => a == e,
        _ => false,
    }
}

fn numerics_close(actual: f64, expected: f64, decimal_scale: Option<i32>) -> bool {
    let abs_diff = (actual - expected).abs();
    if abs_diff < NUMERIC_ABS_EPSILON {
        return true;
    }
    if let Some(places) = decimal_scale
        && places >= MIN_DECIMAL_SCALE
    {
        let unit = 10f64.powi(-places);
        if abs_diff < unit {
            return true;
        }
    }
    let magnitude = actual.abs().max(expected.abs());
    abs_diff < NUMERIC_REL_EPSILON * magnitude
}

/// Declared scale from `decimal(p,s)` / `numeric(p,s)`. Bare `decimal` or
/// `double` has no scale — floats then use abs/rel only.
fn declared_decimal_scale(token: &str) -> Option<i32> {
    let trimmed = token.trim();
    let (base, params) = trimmed.split_once('(')?;
    if !matches!(
        base.trim().to_ascii_lowercase().as_str(),
        "decimal" | "numeric"
    ) {
        return None;
    }
    let inner = params.strip_suffix(')')?.trim();
    let scale_str = inner.split(',').nth(1).unwrap_or(inner).trim();
    scale_str.parse().ok()
}

/// One ULP at a decimal scale is allowed only when both headers declare
/// that same scale. IBM TPC-H goldens type money/`AVG` as `double`, so
/// Mode A stays on abs/rel and cannot hide a cent behind a 2-digit print.
fn shared_decimal_scale(actual_type: Option<&str>, expected_type: Option<&str>) -> Option<i32> {
    let actual_scale = declared_decimal_scale(actual_type?)?;
    let expected_scale = declared_decimal_scale(expected_type?)?;
    (actual_scale == expected_scale).then_some(actual_scale)
}

/// Both sides declared `decimal(p,s)` / `numeric(p,s)`: compare exactly as
/// scaled integers (`i128` holds the 38 digits a typed header may declare),
/// keeping the documented one-ULP allowance at the shared declared scale.
/// `f64` would collapse values that differ past its 15 significant digits
/// (`10000000000000000000000000000000000000` vs `…0001`). `None` when either
/// side is not a plain decimal literal, so the float path decides.
fn decimals_match(actual: &str, expected: &str, shared_scale: Option<i32>) -> Option<bool> {
    let (actual, actual_scale) = parse_scaled_decimal(actual)?;
    let (expected, expected_scale) = parse_scaled_decimal(expected)?;
    let scale = actual_scale.max(expected_scale);
    let rescale = |value: i128, from: i32| {
        10i128
            .checked_pow(u32::try_from(scale - from).ok()?)
            .and_then(|factor| value.checked_mul(factor))
    };
    let diff = rescale(actual, actual_scale)?
        .checked_sub(rescale(expected, expected_scale)?)?
        .unsigned_abs();
    // Strictly less than one unit at the shared scale; with no shared scale
    // (or one below `MIN_DECIMAL_SCALE`) that is exact equality.
    let allowed = match shared_scale.filter(|s| *s >= MIN_DECIMAL_SCALE) {
        Some(shared) if scale > shared => {
            10u128.checked_pow(u32::try_from(scale - shared).ok()?)?
        }
        _ => 1,
    };
    Some(diff < allowed)
}

/// A plain decimal literal (`-12.340`) as its digits and scale: `(-12340, 3)`.
/// Exponents, `NaN` and anything else are `None`.
fn parse_scaled_decimal(text: &str) -> Option<(i128, i32)> {
    let text = text.trim();
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{fraction}");
    let magnitude: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().ok()?
    };
    let scale = i32::try_from(fraction.len()).ok()?;
    Some((if negative { -magnitude } else { magnitude }, scale))
}

/// The scale the engine declares for the actual column (`decimal(p,s)` in
/// its own schema) when the golden is a `double`/`float` rendering of the
/// unrounded value. `DataFusion` returns `AVG(decimal(15,2))` as
/// `decimal(19,6)` and the `DuckDB` goldens print `25.575154611454693`, so
/// the engine's `25.575154` is that value at its declared precision, not a
/// wrong one. The scale comes from the engine's schema, never from the
/// printed string, and there is none when the actual is itself a float.
fn engine_declared_scale(actual_type: Option<&str>, expected_type: Option<&str>) -> Option<i32> {
    let scale = declared_decimal_scale(actual_type?)?;
    let expected = expected_type?;
    (declared_decimal_scale(expected).is_none()
        && matches!(normalize_type(expected), "double" | "float"))
    .then_some(scale)
}

/// Whether `actual` is `expected` rendered at `scale` decimal places: either
/// rounded (`|Δ| ≤ unit / 2`) or truncated toward zero (`|actual| ≤
/// |expected|` by less than one unit, same sign). A value one unit off in the
/// last place is neither — `0.06` for `0.05008…` at scale 2 still fails.
fn rendered_at_declared_scale(actual: f64, expected: f64, scale: Option<i32>) -> bool {
    let Some(scale) = scale.filter(|s| *s >= MIN_DECIMAL_SCALE) else {
        return false;
    };
    let unit = 10f64.powi(-scale);
    if (actual - expected).abs() <= unit / 2.0 {
        return true;
    }
    (actual == 0.0 || actual.signum() == expected.signum())
        && actual.abs() <= expected.abs()
        && expected.abs() - actual.abs() < unit
}

/// Why a golden CSV was rejected at parse time. A zero-byte,
/// headerless, or malformed first line is not an empty result — that
/// needs a typed header and zero data rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseTypedCsvError {
    MissingTypedHeader,
}

impl ParseTypedCsvError {
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::MissingTypedHeader => {
                "missing typed header (`col:type|...`). A zero-byte, headerless, or malformed first line is not an empty result; use a typed header (nonempty names and supported type tokens) with zero data rows"
            }
        }
    }
}

impl std::fmt::Display for ParseTypedCsvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ParseTypedCsvError {}

/// Parse a pipe-delimited expected-output CSV with a typed header
/// (`col:type|col:type|...`) as documented in the IBM TPC-H suite README.
/// Short, wide, or blank data rows are kept so [`compare`] can report a
/// row-count or row-width mismatch instead of silently dropping them.
/// Data-row `|` inside RFC-4180 quotes is part of the field, so
/// `"left|right"|7` is two cells and width checks use that count.
///
/// The first line is a typed header only when every field is `name:type`
/// with a nonempty name and a supported type token (IBM vocabulary,
/// including `decimal(p,s)` / `numeric(p,s)`). A zero-byte file, a
/// headerless data row (including timestamp-like `12:34:56`), or a
/// truncated field (`flag:`, `:`) is an error. A legitimate empty
/// result is a typed header plus its terminating newline and no further
/// line. A blank or whitespace-only line after that header is a data
/// record (typically one empty field), not an empty result.
pub fn parse_typed_csv(text: &str) -> std::result::Result<TableData, ParseTypedCsvError> {
    let mut lines = text.lines();
    let Some(header) = lines.next() else {
        return Err(ParseTypedCsvError::MissingTypedHeader);
    };
    let columns = header
        .split('|')
        .map(parse_typed_header_field)
        .collect::<Option<Vec<_>>>()
        .filter(|columns| !columns.is_empty());
    let Some(columns) = columns else {
        return Err(ParseTypedCsvError::MissingTypedHeader);
    };
    let rows = lines.map(parse_pipe_row).collect();
    Ok(TableData { columns, rows })
}

/// `name:type` with a nonempty name and a supported type token.
fn parse_typed_header_field(field: &str) -> Option<ColumnSpec> {
    let (name, type_str) = field.split_once(':')?;
    let name = name.trim();
    let type_str = type_str.trim();
    if name.is_empty() || !is_supported_type_token(type_str) {
        return None;
    }
    Some(ColumnSpec {
        name: name.to_string(),
        type_token: type_str.to_string(),
    })
}

/// IBM TPC-H README type vocabulary, plus the aliases [`normalize_type`]
/// already accepts. Parameterised `decimal(p,s)` / `numeric(p,s)` require
/// precision `1..=38` and scale `0..=precision`; `varchar(n)` / `char(n)`
/// take a length. Empty, out-of-range, or unknown tokens are not valid.
fn is_supported_type_token(token: &str) -> bool {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return false;
    }
    let (base, params_ok) = match trimmed.find('(') {
        None => (trimmed, true),
        Some(idx) => {
            let base = trimmed[..idx].trim();
            let rest = trimmed[idx..].trim();
            let Some(inner) = rest.strip_prefix('(').and_then(|s| s.strip_suffix(')')) else {
                return false;
            };
            if inner.trim().is_empty() {
                return false;
            }
            (base, parameterized_type_params_ok(base, inner))
        }
    };
    !base.is_empty() && params_ok && is_supported_type_base(base)
}

fn is_supported_type_base(base: &str) -> bool {
    matches!(
        base.to_ascii_lowercase().as_str(),
        "integer"
            | "int"
            | "int32"
            | "i32"
            | "i8"
            | "i16"
            | "int4"
            | "smallint"
            | "tinyint"
            | "bigint"
            | "long"
            | "int64"
            | "i64"
            | "int8"
            | "double"
            | "fp64"
            | "float8"
            | "numeric"
            | "decimal"
            | "number"
            | "float"
            | "fp32"
            | "real"
            | "float4"
            | "boolean"
            | "bool"
            | "string"
            | "varchar"
            | "char"
            | "text"
            | "utf8"
            | "date"
            | "timestamp"
            | "timestamptz"
            | "time"
    )
}

fn parameterized_type_params_ok(base: &str, inner: &str) -> bool {
    match base.to_ascii_lowercase().as_str() {
        "decimal" | "numeric" => decimal_type_params_ok(inner),
        "varchar" | "char" | "timestamp" | "timestamptz" | "time" => {
            inner.trim().parse::<u32>().is_ok()
        }
        _ => false,
    }
}

fn decimal_type_params_ok(inner: &str) -> bool {
    // Arrow `Decimal128` / IBM TPC-H money columns: precision 1..=38.
    const MIN_PRECISION: u32 = 1;
    const MAX_PRECISION: u32 = 38;

    let mut parts = inner.split(',').map(str::trim);
    let Some(precision_str) = parts.next().filter(|part| !part.is_empty()) else {
        return false;
    };
    let Ok(precision) = precision_str.parse::<u32>() else {
        return false;
    };
    if !(MIN_PRECISION..=MAX_PRECISION).contains(&precision) {
        return false;
    }
    match parts.next() {
        None => true,
        Some(scale_str) => {
            let Ok(scale) = scale_str.parse::<u32>() else {
                return false;
            };
            scale <= precision && parts.next().is_none()
        }
    }
}

/// Split a data record on `|` outside RFC-4180 quotes, then unwrap one
/// quote layer. Blank and whitespace-only lines stay one empty field.
fn parse_pipe_row(line: &str) -> Vec<String> {
    split_pipe_record(line)
        .into_iter()
        .map(decode_csv_cell)
        .collect()
}

/// Field boundaries ignore `|` inside `"`; `""` is an escaped quote and
/// does not end the field. Unclosed quotes run to end of line.
fn split_pipe_record(line: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    for (i, ch) in line.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '|' if !in_quotes => {
                fields.push(&line[start..i]);
                start = i + ch.len_utf8();
            }
            _ => {}
        }
    }
    fields.push(&line[start..]);
    fields
}

/// Pipe-delimited IBM goldens quote a field when it is empty/NULL (`""`)
/// or contains the delimiter. Unwrap one layer of RFC-4180 quotes. Only
/// trailing whitespace is dropped, as on the actual side: a leading space
/// is part of the value (q02 `s_comment`, q10 `c_comment`), and trimming
/// it here while the engine keeps it turned two correct results into FAIL.
fn decode_csv_cell(raw: &str) -> String {
    unquote_pipe_cell(raw.trim_end())
}

fn unquote_pipe_cell(cell: &str) -> String {
    if cell.len() >= 2 && cell.starts_with('"') && cell.ends_with('"') {
        cell[1..cell.len() - 1].replace("\"\"", "\"")
    } else {
        cell.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed_numeric_table(type_token: &str, value: &str) -> TableData {
        TableData {
            columns: vec![ColumnSpec {
                name: "n".to_string(),
                type_token: type_token.to_string(),
            }],
            rows: vec![vec![value.to_string()]],
        }
    }

    #[test]
    fn parameterized_decimal_type_is_double_compatible() {
        assert_eq!(normalize_type("decimal(15,6)"), "double");
        assert_eq!(normalize_type("numeric(10, 2)"), "double");
        assert!(types_compatible("decimal(15,2)", "double"));
    }

    #[test]
    fn typed_csv_round_trip_q01_header() {
        let text = "l_returnflag:string|count_order:integer\nA|14876\n";
        let table = parse_typed_csv(text).expect("typed q01 header");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "l_returnflag");
        assert_eq!(table.columns[1].type_token, "integer");
        assert_eq!(table.rows, vec![vec!["A".to_string(), "14876".to_string()]]);
    }

    #[test]
    fn numeric_epsilon_matches_float_formatting() {
        assert!(values_match("380456.0", "380456"));
        assert!(values_match("0.05008133906964134", "0.05008133906964135"));
        assert!(!values_match("1.0", "1.1"));
    }

    #[test]
    fn column_names_are_case_insensitive() {
        let actual = TableData {
            columns: vec![ColumnSpec {
                name: "L_RETURNFLAG".to_string(),
                type_token: "string".to_string(),
            }],
            rows: vec![vec!["A".to_string()]],
        };
        let expected = TableData {
            columns: vec![ColumnSpec {
                name: "l_returnflag".to_string(),
                type_token: "varchar".to_string(),
            }],
            rows: vec![vec!["A".to_string()]],
        };
        assert_eq!(compare(&actual, &expected), None);
    }

    #[test]
    fn count_width_integer_and_bigint_are_compatible() {
        let actual = TableData {
            columns: vec![ColumnSpec {
                name: "count_order".to_string(),
                type_token: "bigint".to_string(),
            }],
            rows: vec![vec!["14876".to_string()]],
        };
        let expected = TableData {
            columns: vec![ColumnSpec {
                name: "count_order".to_string(),
                type_token: "integer".to_string(),
            }],
            rows: vec![vec!["14876".to_string()]],
        };
        assert_eq!(compare(&actual, &expected), None);
    }

    #[test]
    fn string_versus_integer_type_compares_values() {
        // q22 country codes: IBM allows number-vs-string; values decide.
        let actual = TableData {
            columns: vec![ColumnSpec {
                name: "cntrycode".to_string(),
                type_token: "string".to_string(),
            }],
            rows: vec![vec!["13".to_string()]],
        };
        let expected = TableData {
            columns: vec![ColumnSpec {
                name: "cntrycode".to_string(),
                type_token: "integer".to_string(),
            }],
            rows: vec![vec!["13".to_string()]],
        };
        assert_eq!(compare(&actual, &expected), None);

        let wrong = TableData {
            columns: actual.columns,
            rows: vec![vec!["99".to_string()]],
        };
        assert!(matches!(
            compare(&wrong, &expected),
            Some(CompareMismatch::Value {
                row: 0,
                column: 0,
                ..
            })
        ));
    }

    #[test]
    fn plan_alias_versus_expression_name_is_not_a_mismatch() {
        let actual = TableData {
            columns: vec![ColumnSpec {
                name: "TOTAL_VALUE".to_string(),
                type_token: "double".to_string(),
            }],
            rows: vec![vec!["13271249.89".to_string()]],
        };
        let expected = TableData {
            columns: vec![ColumnSpec {
                name: "value".to_string(),
                type_token: "double".to_string(),
            }],
            rows: vec![vec!["13271249.89".to_string()]],
        };
        assert_eq!(compare(&actual, &expected), None);
    }

    /// A golden's leading space survives decode; only the trailing pad is
    /// dropped, as on the actual side.
    #[test]
    fn golden_leading_space_survives_decode() {
        let golden = parse_typed_csv("c:string\n foxes boost \n").expect("typed golden");
        assert_eq!(golden.rows, vec![vec![" foxes boost".to_string()]]);
        let same = TableData {
            columns: golden.columns.clone(),
            rows: vec![vec![" foxes boost".to_string()]],
        };
        assert_eq!(compare(&same, &golden), None);
        let stripped = TableData {
            columns: golden.columns.clone(),
            rows: vec![vec!["foxes boost".to_string()]],
        };
        assert!(matches!(
            compare(&stripped, &golden),
            Some(CompareMismatch::Value {
                row: 0,
                column: 0,
                ..
            })
        ));
    }

    /// The engine-declared decimal scale bounds a `double` golden: rounded or
    /// truncated at that scale passes, one unit off or a float actual does not.
    #[test]
    fn engine_declared_scale_bounds_double_goldens() {
        let dec6 = Some("decimal(19,6)");
        let dbl = Some("double");
        // q01 AVG_QTY / AVG_PRICE / AVG_DISC: truncated or rounded at scale 6.
        assert!(cells_match("25.575154", "25.575154611454693", dec6, dbl));
        assert!(cells_match("35785.709306", "35785.709306937264", dec6, dbl));
        assert!(cells_match("0.050081", "0.05008133906964134", dec6, dbl));
        // Off by more than the last place.
        assert!(!cells_match("25.575200", "25.575154611454693", dec6, dbl));
        // One unit off in the last place is not a rendering of the value.
        assert!(!cells_match(
            "0.06",
            "0.05008133906964134",
            Some("decimal(15,2)"),
            dbl
        ));
        assert!(cells_match(
            "0.05",
            "0.05008133906964134",
            Some("decimal(15,2)"),
            dbl
        ));
        // A float actual has no declared scale: the printed length is not a tolerance.
        assert!(!cells_match("25.575154", "25.575154611454693", dbl, dbl));
        // Scale 1 is float formatting, not a declared scale.
        assert!(!cells_match(
            "25.5",
            "25.575154611454693",
            Some("decimal(15,1)"),
            dbl
        ));
    }

    /// Whitespace-only is a value, not NULL: a one-space actual against a
    /// NULL golden fails on an integer column; on a string column the
    /// trailing-pad rule still matches it to an empty golden.
    #[test]
    fn whitespace_only_is_a_value_not_null() {
        let int = Some("integer");
        assert!(!cells_match(" ", "", int, int));
        assert!(!cells_match("", " ", int, int));
        assert!(cells_match("", "", int, int));
        assert!(values_match(" ", ""));
    }

    /// The numeric side decides a numeric/string pair whichever side it is
    /// on: `13` and `13.0` are equal integers, `13` and `14` are not.
    #[test]
    fn numeric_string_pairs_compare_numerically_in_both_directions() {
        let int = Some("integer");
        let string = Some("string");
        assert!(cells_match("13", "13.0", int, string));
        assert!(cells_match("13.0", "13", string, int));
        assert!(!cells_match("13", "14", int, string));
        assert!(!cells_match("14", "13", string, int));
        // Two strings stay an exact (pad-trimmed) string comparison.
        assert!(!cells_match("13", "13.0", string, string));
    }

    /// Two declared decimals never go through `f64`: values that differ in
    /// the 38th digit mismatch, trailing zeros are equal, one unit at the
    /// shared scale is not a match, and a non-literal falls back to floats.
    #[test]
    fn declared_decimals_compare_exactly() {
        let dec38 = Some("decimal(38,0)");
        assert!(!cells_match(
            "10000000000000000000000000000000000000",
            "10000000000000000000000000000000000001",
            dec38,
            dec38
        ));
        assert!(cells_match(
            "10000000000000000000000000000000000000",
            "10000000000000000000000000000000000000",
            dec38,
            dec38
        ));
        let dec2 = Some("decimal(15,2)");
        assert!(cells_match("1.10", "1.1", dec2, dec2));
        assert!(cells_match("-0.00", "0", dec2, dec2));
        assert!(!cells_match("1.10", "1.11", dec2, dec2));
        let dec6 = Some("decimal(19,6)");
        assert!(!cells_match("25.575154", "25.575155", dec6, dec6));
        // A golden printed past the shared scale is within one ULP of it.
        assert!(cells_match("25.575154", "25.5751546", dec6, dec6));
        assert!(!cells_match("25.575154", "25.5751556", dec6, dec6));
        // Not plain literals: the float path decides.
        assert!(cells_match("1e2", "100", dec2, dec2));
        assert_eq!(parse_scaled_decimal("-12.340"), Some((-12340, 3)));
        assert_eq!(parse_scaled_decimal("1e2"), None);
        assert_eq!(parse_scaled_decimal("."), None);
    }

    /// Identical text certifies nothing on a numeric column: the value must
    /// parse. Text kinds still match on identical text.
    #[test]
    fn identical_malformed_numbers_do_not_match() {
        let int = Some("integer");
        let dbl = Some("double");
        let string = Some("string");
        assert!(!cells_match("not-a-number", "not-a-number", string, int));
        assert!(!cells_match("not-a-number", "not-a-number", int, string));
        assert!(!cells_match("x", "x", int, int));
        assert!(!cells_match("abc", "abc", dbl, dbl));
        assert!(cells_match("abc", "abc", string, string));
        assert!(cells_match("13", "13", int, string));
        assert!(cells_match("NaN", "NaN", dbl, dbl));
        assert!(cells_match("inf", "inf", dbl, dbl));
        assert!(cells_match("", "", int, int));
    }

    #[test]
    fn char_padding_is_trailing_only_for_string_cells() {
        // q02 `s_comment` leading space is significant; both-end trim false-PASSed.
        assert!(!values_match(" foxes boost", "foxes boost"));
        assert!(!values_match("foxes boost", " foxes boost"));
        // q15 `s_address` CHAR / DuckDB trailing pad.
        assert!(values_match("TZoQwNFFO ", "TZoQwNFFO"));
        assert!(values_match("TZoQwNFFO", "TZoQwNFFO "));
        assert!(!values_match("alpha", "beta"));
    }

    #[test]
    fn typed_string_preserves_leading_space() {
        let actual = TableData {
            columns: vec![ColumnSpec {
                name: "s_comment".to_string(),
                type_token: "string".to_string(),
            }],
            rows: vec![vec!["foxes boost".to_string()]],
        };
        let expected = TableData {
            columns: actual.columns.clone(),
            rows: vec![vec![" foxes boost".to_string()]],
        };
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::Value {
                row: 0,
                column: 0,
                ..
            })
        ));
    }

    #[test]
    fn numeric_epsilon_covers_q06_decimal_rounding() {
        // IBM README ε = 1e-9; measured |Δ| ≈ 1.16e-9. Harness abs ε = 1e-8.
        assert!(values_match("1193053.2253", "1193053.225299999"));
        assert!(!values_match("1.0", "1.1"));
    }

    #[test]
    fn relative_epsilon_covers_q01_sum_decimal_conversion() {
        // `|Δ| ≈ 1.2e-6` / `5.3e8` ≈ `2e-15` is under relative `1e-14`.
        assert!(values_match("532348211.65", "532348211.6499988"));
        // q01 `sum_charge`: DF decimal vs DuckDB float64 (`|Δ| ≈ 1.4e-6`).
        assert!(values_match("526165934.000839", "526165934.0008404"));
        assert!(!values_match("14876", "14877"));
    }

    #[test]
    fn declared_decimal_scale_covers_q01_avg() {
        // Shared `decimal(15,6)` ULP (`1e-6`); untyped/`double` uses abs/rel only.
        let actual = typed_numeric_table("decimal(15,6)", "25.575154");
        let expected = typed_numeric_table("decimal(15,6)", "25.575154611454693");
        assert_eq!(compare(&actual, &expected), None);

        let actual = typed_numeric_table("decimal(15,6)", "0.050081");
        let expected = typed_numeric_table("decimal(15,6)", "0.05008133906964134");
        assert_eq!(compare(&actual, &expected), None);

        let actual = typed_numeric_table("decimal(15,6)", "25.575154");
        let expected = typed_numeric_table("decimal(15,6)", "26.575154611454693");
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::Value { .. })
        ));
        assert!(!values_match("25.575154", "25.575154611454693"));
        assert!(!values_match("0.050081", "0.05008133906964134"));
    }

    #[test]
    fn printed_scale_does_not_accept_tenth_errors() {
        // Truncation at printed scale 1 accepted `1.09` vs `1.0`.
        assert!(!values_match("1.09", "1.0"));
        assert!(!values_match("1.0", "1.09"));
    }

    #[test]
    fn printed_fractional_length_does_not_match_wrong_avg() {
        // Actual print scale 2 used to accept Δ ≈ 0.01 against a q01-like AVG.
        assert!(!values_match("0.06", "0.05008133906964134"));
        assert!(!values_match("0.05008133906964134", "0.06"));

        let actual = typed_numeric_table("double", "0.06");
        let expected = typed_numeric_table("double", "0.05008133906964134");
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::Value { .. })
        ));
    }

    #[test]
    fn printed_fractional_length_does_not_hide_a_cent() {
        // Fails abs/rel; printed scale 2 used to accept Δ ≈ 0.01.
        assert!(!values_match("532348211.64", "532348211.6499988"));
        assert!(!values_match("532348211.6499988", "532348211.64"));

        let actual = typed_numeric_table("double", "532348211.64");
        let expected = typed_numeric_table("double", "532348211.6499988");
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::Value { .. })
        ));
    }

    #[test]
    fn decimal_scale_does_not_accept_half_unit_money_error() {
        // Relative `1e-9` of ~`5.3e8` is ~`0.53` and accepted Δ ≈ `0.50`.
        assert!(!values_match("532348211.15", "532348211.6499988"));
        assert!(!values_match("532348211.6499988", "532348211.15"));
    }

    #[test]
    fn integer_counts_compare_exactly() {
        // Relative ε accepted `COUNT` `1000000001` vs `1000000000`.
        assert!(!values_match("1000000001", "1000000000"));
        assert!(!values_match("1000000000", "1000000001"));

        let actual = TableData {
            columns: vec![ColumnSpec {
                name: "count_order".to_string(),
                type_token: "bigint".to_string(),
            }],
            rows: vec![vec!["1000000001".to_string()]],
        };
        let expected = TableData {
            columns: vec![ColumnSpec {
                name: "count_order".to_string(),
                type_token: "integer".to_string(),
            }],
            rows: vec![vec!["1000000000".to_string()]],
        };
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::Value {
                row: 0,
                column: 0,
                ..
            })
        ));
    }

    #[test]
    fn quoted_empty_matches_empty_null_cell() {
        // CSV `""` is quoted-empty and decodes to the empty string (NULL).
        let parsed = parse_typed_csv("avg_yearly:double\n\"\"\n").expect("quoted-empty golden");
        assert_eq!(parsed.rows, vec![vec![String::new()]]);

        let actual = TableData {
            columns: parsed.columns.clone(),
            rows: vec![vec![String::new()]],
        };
        assert_eq!(compare(&actual, &parsed), None);
        assert!(values_match("", ""));
    }

    #[test]
    fn incomplete_golden_row_is_a_mismatch() {
        let expected = parse_typed_csv("a:integer|b:integer\n1\n").expect("short-row golden");
        assert_eq!(expected.columns.len(), 2);
        assert_eq!(expected.rows, vec![vec!["1".to_string()]]);

        let actual = TableData {
            columns: expected.columns.clone(),
            rows: vec![vec!["1".to_string(), "999".to_string()]],
        };
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::RowWidth {
                row: 0,
                actual: 2,
                expected: 1,
                columns: 2
            })
        ));
    }

    #[test]
    fn extra_actual_fields_are_a_mismatch() {
        let expected = parse_typed_csv("a:integer|b:integer\n1|2\n").expect("two-column golden");
        let actual = TableData {
            columns: expected.columns.clone(),
            rows: vec![vec!["1".to_string(), "2".to_string(), "999".to_string()]],
        };
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::RowWidth {
                row: 0,
                actual: 3,
                expected: 2,
                columns: 2
            })
        ));
    }

    #[test]
    fn row_count_mismatch_is_reported() {
        let actual = TableData {
            columns: vec![ColumnSpec {
                name: "x".to_string(),
                type_token: "integer".to_string(),
            }],
            rows: vec![vec!["1".to_string()]],
        };
        let expected = TableData {
            columns: actual.columns.clone(),
            rows: vec![vec!["1".to_string()], vec!["2".to_string()]],
        };
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::RowCount {
                actual: 1,
                expected: 2
            })
        ));
    }

    #[test]
    fn zero_byte_golden_is_a_parse_error() {
        assert_eq!(
            parse_typed_csv(""),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
        assert_eq!(
            parse_typed_csv("   \n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
        assert!(
            ParseTypedCsvError::MissingTypedHeader
                .message()
                .contains("typed header")
        );
    }

    #[test]
    fn headerless_golden_is_a_parse_error() {
        assert_eq!(
            parse_typed_csv("1|2\n3|4\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
    }

    #[test]
    fn truncated_typed_header_fields_are_parse_errors() {
        assert_eq!(
            parse_typed_csv("flag:\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
        assert_eq!(
            parse_typed_csv(":\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
    }

    #[test]
    fn headerless_timestamp_like_line_is_a_parse_error() {
        // A colon is not enough: `12:34:56` is a data value, not `name:type`.
        assert_eq!(
            parse_typed_csv("12:34:56\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
    }

    #[test]
    fn parameterized_decimal_header_is_typed() {
        let table =
            parse_typed_csv("avg_qty:decimal(15,6)\n").expect("parameterized decimal header");
        assert_eq!(table.columns[0].name, "avg_qty");
        assert_eq!(table.columns[0].type_token, "decimal(15,6)");
        assert!(table.rows.is_empty());
    }

    #[test]
    fn invalid_decimal_params_are_parse_errors() {
        // Precision 1..=38 and scale 0..=precision; invalid params are not a
        // typed header and must not type-check as `double`.
        assert_eq!(
            parse_typed_csv("n:decimal(-1,2)\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
        assert_eq!(
            parse_typed_csv("n:decimal(0,2)\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
        assert_eq!(
            parse_typed_csv("n:decimal(5,6)\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
        assert_eq!(
            parse_typed_csv("n:decimal()\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
        assert_eq!(
            parse_typed_csv("n:numeric(0,2)\n"),
            Err(ParseTypedCsvError::MissingTypedHeader)
        );
    }

    #[test]
    fn header_only_decimal_golden_passes_empty_actual() {
        let expected = parse_typed_csv("n:decimal(15,2)\n").expect("header-only decimal golden");
        assert_eq!(expected.columns[0].type_token, "decimal(15,2)");
        assert!(expected.rows.is_empty());

        let actual = TableData {
            columns: expected.columns.clone(),
            rows: Vec::new(),
        };
        assert_eq!(compare(&actual, &expected), None);
    }

    #[test]
    fn zero_byte_golden_cannot_pass_against_empty_actual() {
        let actual = TableData::default();
        let expected = TableData::default();
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::MissingTypedHeader)
        ));
    }

    #[test]
    fn header_only_golden_passes_empty_actual_with_matching_schema() {
        let expected =
            parse_typed_csv("flag:string|n:integer\n").expect("header-only typed golden");
        assert_eq!(expected.columns.len(), 2);
        assert!(expected.rows.is_empty());

        let actual = TableData {
            columns: expected.columns.clone(),
            rows: Vec::new(),
        };
        assert_eq!(compare(&actual, &expected), None);

        // Empty execution that dropped schema cannot type-check a header-only
        // golden — Mode A must preserve the DataFrame schema on zero batches.
        let schema_less = TableData::default();
        assert!(matches!(
            compare(&schema_less, &expected),
            Some(CompareMismatch::ColumnCount {
                actual: 0,
                expected: 2
            })
        ));
    }

    #[test]
    fn blank_data_record_after_header_does_not_pass_empty_actual() {
        // A whitespace-only line after the typed header is a data record,
        // not a dropped row that would false-PASS against empty execution.
        let expected =
            parse_typed_csv("a:integer|b:integer\n \n").expect("blank record after header");
        assert_eq!(expected.columns.len(), 2);
        assert_eq!(expected.rows, vec![vec![String::new()]]);

        let empty_line = parse_typed_csv("a:integer|b:integer\n\n").expect("empty-line record");
        assert_eq!(empty_line.rows, vec![vec![String::new()]]);

        let actual = TableData {
            columns: expected.columns.clone(),
            rows: Vec::new(),
        };
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::RowCount {
                actual: 0,
                expected: 1
            })
        ));
    }

    #[test]
    fn blank_data_record_between_rows_does_not_pass() {
        let expected = parse_typed_csv("a:integer|b:integer\n1|2\n \n3|4\n")
            .expect("blank record between populated rows");
        assert_eq!(
            expected.rows,
            vec![
                vec!["1".to_string(), "2".to_string()],
                vec![String::new()],
                vec!["3".to_string(), "4".to_string()],
            ]
        );

        let actual = TableData {
            columns: expected.columns.clone(),
            rows: vec![
                vec!["1".to_string(), "2".to_string()],
                vec!["3".to_string(), "4".to_string()],
            ],
        };
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::RowCount {
                actual: 2,
                expected: 3
            })
        ));

        // Trailing whitespace-only line after a matching row must not drop
        // and false-PASS against the populated row alone.
        let trailing = parse_typed_csv("a:integer|b:integer\n1|2\n \n")
            .expect("blank record after populated row");
        assert_eq!(trailing.rows.len(), 2);
        let one_row = TableData {
            columns: trailing.columns.clone(),
            rows: vec![vec!["1".to_string(), "2".to_string()]],
        };
        assert!(matches!(
            compare(&one_row, &trailing),
            Some(CompareMismatch::RowCount {
                actual: 1,
                expected: 2
            })
        ));
    }

    #[test]
    fn quoted_embedded_pipe_is_one_field() {
        let expected =
            parse_typed_csv("a:string|b:integer\n\"left|right\"|7\n").expect("quoted pipe golden");
        assert_eq!(
            expected.rows,
            vec![vec!["left|right".to_string(), "7".to_string()]]
        );

        let actual = TableData {
            columns: expected.columns.clone(),
            rows: vec![vec!["left|right".to_string(), "7".to_string()]],
        };
        assert_eq!(compare(&actual, &expected), None);
    }

    #[test]
    fn quoted_delimiter_does_not_inflate_short_row_width() {
        // Naive `|` split of `"left|right"` is two fields and used to
        // bypass the incomplete-row guard on a two-column schema.
        let expected = parse_typed_csv("a:string|b:integer\n\"left|right\"\n")
            .expect("quoted short-row golden");
        assert_eq!(expected.rows, vec![vec!["left|right".to_string()]]);

        let actual = TableData {
            columns: expected.columns.clone(),
            rows: vec![vec!["left|right".to_string(), "7".to_string()]],
        };
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::RowWidth {
                row: 0,
                actual: 2,
                expected: 1,
                columns: 2
            })
        ));
    }

    #[test]
    fn quoted_empty_beside_quoted_pipe_stays_null() {
        let parsed = parse_typed_csv("a:string|b:string|c:integer\n\"left|right\"|\"\"|7\n")
            .expect("quoted pipe and quoted-empty");
        assert_eq!(
            parsed.rows,
            vec![vec![
                "left|right".to_string(),
                String::new(),
                "7".to_string()
            ]]
        );
    }

    #[test]
    fn escaped_quote_literal_does_not_match_null() {
        // CSV `""""""` is one quoted field of two escaped quotes → literal `""`.
        let expected =
            parse_typed_csv("a:string|b:integer\n\"\"\"\"\"\"|7\n").expect("escaped-quote golden");
        assert_eq!(
            expected.rows,
            vec![vec!["\"\"".to_string(), "7".to_string()]]
        );

        let null_actual = TableData {
            columns: expected.columns.clone(),
            rows: vec![vec![String::new(), "7".to_string()]],
        };
        assert!(matches!(
            compare(&null_actual, &expected),
            Some(CompareMismatch::Value {
                row: 0,
                column: 0,
                ..
            })
        ));

        let empty_expected =
            parse_typed_csv("a:string|b:integer\n\"\"|7\n").expect("quoted-empty golden");
        assert_eq!(
            empty_expected.rows,
            vec![vec![String::new(), "7".to_string()]]
        );
        assert_eq!(compare(&null_actual, &empty_expected), None);

        let quote_actual = TableData {
            columns: empty_expected.columns.clone(),
            rows: vec![vec!["\"\"".to_string(), "7".to_string()]],
        };
        assert!(matches!(
            compare(&quote_actual, &empty_expected),
            Some(CompareMismatch::Value {
                row: 0,
                column: 0,
                ..
            })
        ));

        assert!(!values_match("", "\"\""));
        assert!(!values_match("\"\"", ""));
    }
}
