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
//! - String cells are compared after trimming `CHAR` pad / loader whitespace.
//! - Numerics: absolute ε = `1e-8`, or relative `1e-9` of magnitude, or
//!   agreement at the coarser printed fractional scale (`decimal` scale
//!   6 vs `DuckDB` float). IBM README is absolute `1e-9` only. Off-by-one
//!   `COUNT` still fails.
//!
//! Not lifted: empty vs quoted-empty (`""`), row-count misses, `string` vs
//! `integer` (q17 / q21 / q22).

/// Absolute numeric floor. IBM documents `1e-9`; `1e-8` covers q06's
/// `DuckDB`-vs-`DataFusion` rounding (Δ ≈ 1.16e-9).
const NUMERIC_ABS_EPSILON: f64 = 1e-8;

/// Relative tolerance for large `DECIMAL` sums (q01 `sum_base_price`
/// Δ ≈ 1.2e-6 on ~5.3e8). An off-by-one `COUNT` still misses.
const NUMERIC_REL_EPSILON: f64 = 1e-9;

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
    Value {
        row: usize,
        column: usize,
        actual: String,
        expected: String,
    },
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
            Self::Value {
                row,
                column,
                actual,
                expected,
            } => write!(f, "cell ({row},{column}) '{actual}' != '{expected}'"),
        }
    }
}

#[must_use]
pub fn compare(actual: &TableData, expected: &TableData) -> Option<CompareMismatch> {
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
    for (row_idx, (a_row, e_row)) in actual.rows.iter().zip(expected.rows.iter()).enumerate() {
        for (col_idx, (a_val, e_val)) in a_row.iter().zip(e_row.iter()).enumerate() {
            if !values_match(a_val, e_val) {
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
    match token.to_ascii_lowercase().as_str() {
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
/// `string` vs `integer` is not compatible (q22 stays FAIL).
#[must_use]
pub fn types_compatible(actual: &str, expected: &str) -> bool {
    let a = normalize_type(actual);
    let e = normalize_type(expected);
    a == e || matches!((a, e), ("integer", "bigint") | ("bigint", "integer"))
}

#[must_use]
pub fn values_match(actual: &str, expected: &str) -> bool {
    if actual == expected {
        return true;
    }
    if let (Ok(a), Ok(e)) = (actual.parse::<f64>(), expected.parse::<f64>()) {
        if a.is_nan() && e.is_nan() {
            return true;
        }
        return numerics_close(a, e, actual, expected);
    }
    let a_lower = actual.to_ascii_lowercase();
    let e_lower = expected.to_ascii_lowercase();
    if matches!(a_lower.as_str(), "true" | "false") && matches!(e_lower.as_str(), "true" | "false")
    {
        return a_lower == e_lower;
    }
    // Isthmus `CHAR` / fixed-char padding vs trimmed `DuckDB` goldens (q02, q10, q15).
    // Quoted-empty `""` is not unquoted to empty — q17 stays a Value miss.
    actual.trim() == expected.trim()
}

fn numerics_close(actual: f64, expected: f64, actual_s: &str, expected_s: &str) -> bool {
    let abs_diff = (actual - expected).abs();
    if abs_diff < NUMERIC_ABS_EPSILON {
        return true;
    }
    let magnitude = actual.abs().max(expected.abs());
    if abs_diff < NUMERIC_REL_EPSILON * magnitude {
        return true;
    }
    same_at_printed_scale(actual, expected, actual_s, expected_s)
}

/// `DataFusion` prints `decimal` at its scale (q01 `AVG_*` is 6 places);
/// `DuckDB` goldens are float64. Agree if truncation *or* rounding at the
/// coarser printed scale matches. Integers (no fraction) are not scaled.
fn same_at_printed_scale(actual: f64, expected: f64, actual_s: &str, expected_s: &str) -> bool {
    let Some(actual_places) = frac_digits(actual_s) else {
        return false;
    };
    let Some(expected_places) = frac_digits(expected_s) else {
        return false;
    };
    let places = actual_places.min(expected_places);
    if places <= 0 {
        return false;
    }
    let factor = 10f64.powi(places);
    let a_scaled = actual * factor;
    let e_scaled = expected * factor;
    // Scaled values are integer-valued; `< 0.5` is equality without `==` on `f64`.
    let trunc_match = (a_scaled.trunc() - e_scaled.trunc()).abs() < 0.5;
    let round_match = (a_scaled.round() - e_scaled.round()).abs() < 0.5;
    trunc_match || round_match
}

fn frac_digits(value: &str) -> Option<i32> {
    let frac = value.trim().split_once('.')?.1;
    if frac.is_empty() {
        return None;
    }
    i32::try_from(frac.len()).ok()
}

/// Parse a pipe-delimited expected-output CSV with a typed header
/// (`col:type|col:type|...`) as documented in the IBM TPC-H suite README.
#[must_use]
pub fn parse_typed_csv(text: &str) -> TableData {
    let mut lines = text.lines().peekable();
    let Some(header) = lines.next() else {
        return TableData {
            columns: Vec::new(),
            rows: Vec::new(),
        };
    };
    let fields: Vec<&str> = header.split('|').collect();
    let has_typed_header = fields.iter().all(|f| f.contains(':'));
    let columns = if has_typed_header {
        fields
            .iter()
            .map(|f| {
                let (name, type_str) = f.split_once(':').unwrap_or((f, "varchar"));
                ColumnSpec {
                    name: name.trim().to_string(),
                    type_token: type_str.trim().to_string(),
                }
            })
            .collect()
    } else {
        fields
            .iter()
            .enumerate()
            .map(|(i, _)| ColumnSpec {
                name: format!("column_{}", i + 1),
                type_token: "varchar".to_string(),
            })
            .collect()
    };
    let data_lines = if has_typed_header {
        lines
    } else {
        // Header was actually the first data row; re-parse including it.
        return parse_untyped(text);
    };
    let rows = data_lines
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.split('|')
                .map(str::trim)
                .map(ToString::to_string)
                .collect()
        })
        .collect();
    TableData { columns, rows }
}

fn parse_untyped(text: &str) -> TableData {
    let rows: Vec<Vec<String>> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.split('|')
                .map(str::trim)
                .map(ToString::to_string)
                .collect()
        })
        .collect();
    let ncols = rows.first().map_or(0, Vec::len);
    let columns = (1..=ncols)
        .map(|i| ColumnSpec {
            name: format!("column_{i}"),
            type_token: "varchar".to_string(),
        })
        .collect();
    TableData { columns, rows }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_csv_round_trip_q01_header() {
        let text = "l_returnflag:string|count_order:integer\nA|14876\n";
        let table = parse_typed_csv(text);
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
    fn string_versus_integer_type_is_not_compatible() {
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
        assert!(matches!(
            compare(&actual, &expected),
            Some(CompareMismatch::ColumnType { index: 0, .. })
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

    #[test]
    fn char_padding_is_trimmed_for_string_cells() {
        assert!(values_match(" foxes boost", "foxes boost"));
        assert!(values_match("TZoQwNFFO ", "TZoQwNFFO"));
        assert!(!values_match("", "\"\""));
        assert!(!values_match("alpha", "beta"));
    }

    #[test]
    fn numeric_epsilon_covers_q06_decimal_rounding() {
        // IBM README ε = 1e-9; measured |Δ| ≈ 1.16e-9. Harness abs ε = 1e-8.
        assert!(values_match("1193053.2253", "1193053.225299999"));
        assert!(!values_match("1.0", "1.1"));
    }

    #[test]
    fn numeric_relative_epsilon_covers_q01_decimal_sum() {
        assert!(values_match("532348211.65", "532348211.6499988"));
        assert!(!values_match("14876", "14877"));
    }

    #[test]
    fn printed_scale_covers_q01_avg_decimal_vs_float() {
        // decimal scale 6 vs DuckDB float64. Truncation or rounding at
        // the coarser printed scale; not a global ε widen.
        assert!(values_match("25.575154", "25.575154611454693"));
        assert!(values_match("0.050081", "0.05008133906964134"));
        assert!(!values_match("25.575154", "26.575154611454693"));
        assert!(!values_match("0.050081", "0.15008133906964134"));
    }

    #[test]
    fn quoted_empty_does_not_match_empty_cell() {
        let actual = TableData {
            columns: vec![ColumnSpec {
                name: "avg_yearly".to_string(),
                type_token: "double".to_string(),
            }],
            rows: vec![vec![String::new()]],
        };
        let expected = TableData {
            columns: actual.columns.clone(),
            rows: vec![vec!["\"\"".to_string()]],
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
}
