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

//! Result comparison matching the IBM TPC-H suite rules
//! (`test-suites/tpch/README.md` at tag v0.1.1):
//! row count, column count, normalised types, then per-cell values with
//! absolute epsilon `1e-9` for numerics. Column *names* are compared
//! case-insensitively because Isthmus plans emit `L_RETURNFLAG` while the
//! golden CSVs use `l_returnflag`. The IBM Rust SDK comparator does not
//! check names at all.

const NUMERIC_EPSILON: f64 = 1e-9;

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
    ColumnName {
        index: usize,
        actual: String,
        expected: String,
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
            Self::ColumnName {
                index,
                actual,
                expected,
            } => write!(f, "column {index} name '{actual}' != '{expected}'"),
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
        if !a_col.name.eq_ignore_ascii_case(&e_col.name) {
            return Some(CompareMismatch::ColumnName {
                index,
                actual: a_col.name.clone(),
                expected: e_col.name.clone(),
            });
        }
        if normalize_type(&a_col.type_token) != normalize_type(&e_col.type_token) {
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

#[must_use]
pub fn values_match(actual: &str, expected: &str) -> bool {
    if actual == expected {
        return true;
    }
    if let (Ok(a), Ok(e)) = (actual.parse::<f64>(), expected.parse::<f64>()) {
        if a.is_nan() && e.is_nan() {
            return true;
        }
        return (a - e).abs() < NUMERIC_EPSILON;
    }
    let a_lower = actual.to_ascii_lowercase();
    let e_lower = expected.to_ascii_lowercase();
    if matches!(a_lower.as_str(), "true" | "false") && matches!(e_lower.as_str(), "true" | "false")
    {
        return a_lower == e_lower;
    }
    false
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
