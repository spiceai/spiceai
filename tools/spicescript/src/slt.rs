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

//! sqllogictest files as a corpus over a real source.
//!
//! A file's `statement` records run on the source (they create and fill its
//! tables); its `query` records run through Spice, over datasets the spicepod
//! declares on those tables, and are checked against the answers recorded in
//! the file. Values are formatted the way sqllogictest prints them: by the
//! column type the record declares (`I` integer, `R` three decimals, `T`
//! text), `NULL`, and `(empty)` for an empty string.
//!
//! When Spice's answer differs, the same query runs on the source. If the
//! source gives the recorded answer, the difference is Spice's; if the source
//! disagrees too, it is the dialect, and is reported apart.

use std::{path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use arrow::{
    array::{Array, RecordBatch},
    util::display::{ArrayFormatter, FormatOptions},
};
use md5::{Digest, Md5};
use sqllogictest::{Condition, DefaultColumnType, QueryExpect, Record, SortMode, StatementExpect};

use crate::source::TextResult;

/// The records of a file that apply to a run with these labels, in order.
pub enum Item {
    Statement {
        loc: String,
        sql: String,
        may_fail: bool,
    },
    Query(SltQuery),
}

pub struct SltQuery {
    pub loc: String,
    pub sql: String,
    pub types: Vec<DefaultColumnType>,
    pub sort: SortMode,
    pub expected: Vec<String>,
    pub expect_error: bool,
}

pub struct Loaded {
    pub items: Vec<Item>,
    /// Records skipped by `skipif`/`onlyif` for these labels.
    pub skipped: usize,
}

pub fn load(path: &Path, labels: &[String]) -> Result<Loaded> {
    let records = sqllogictest::parse_file::<DefaultColumnType>(path)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    let skips = |conditions: &[Condition]| {
        conditions.iter().any(|c| match c {
            Condition::OnlyIf { label } => !labels.contains(label),
            Condition::SkipIf { label } => labels.contains(label),
        })
    };
    let mut items = Vec::new();
    let mut skipped = 0;
    for record in records {
        match record {
            Record::Statement {
                loc,
                conditions,
                sql,
                expected,
                ..
            } => {
                if skips(&conditions) {
                    skipped += 1;
                    continue;
                }
                items.push(Item::Statement {
                    loc: loc.to_string(),
                    sql,
                    may_fail: matches!(expected, StatementExpect::Error(_)),
                });
            }
            Record::Query {
                loc,
                conditions,
                sql,
                expected,
                ..
            } => {
                if skips(&conditions) {
                    skipped += 1;
                    continue;
                }
                let query = match expected {
                    QueryExpect::Results {
                        types,
                        sort_mode,
                        results,
                        ..
                    } => SltQuery {
                        loc: loc.to_string(),
                        sql,
                        types,
                        sort: sort_mode.unwrap_or(SortMode::NoSort),
                        expected: results,
                        expect_error: false,
                    },
                    QueryExpect::Error(_) => SltQuery {
                        loc: loc.to_string(),
                        sql,
                        types: Vec::new(),
                        sort: SortMode::NoSort,
                        expected: Vec::new(),
                        expect_error: true,
                    },
                };
                items.push(Item::Query(query));
            }
            Record::Include { .. } | Record::System { .. } => {
                bail!(
                    "{}: include and system records are not supported",
                    path.display()
                )
            }
            _ => {}
        }
    }
    Ok(Loaded { items, skipped })
}

/// Formats one value the way sqllogictest prints a column of type `ty`.
fn format_value(value: Option<&str>, ty: &DefaultColumnType) -> String {
    let Some(value) = value else {
        return "NULL".to_string();
    };
    match ty {
        DefaultColumnType::Integer => match value {
            "true" | "t" => "1".to_string(),
            "false" | "f" => "0".to_string(),
            v if v.parse::<i64>().is_ok() => v.to_string(),
            // A real in an integer column is truncated toward zero, as
            // SQLite's sqlite3_column_int64 does.
            #[expect(clippy::cast_possible_truncation, reason = "truncation is the rule")]
            v => v
                .parse::<f64>()
                .map_or_else(|_| v.to_string(), |f| format!("{}", f.trunc() as i64)),
        },
        DefaultColumnType::FloatingPoint => value
            .parse::<f64>()
            .map_or_else(|_| value.to_string(), |f| format!("{f:.3}")),
        DefaultColumnType::Text | DefaultColumnType::Any => {
            if value.is_empty() {
                "(empty)".to_string()
            } else {
                value.to_string()
            }
        }
    }
}

/// Renders Spice's answer as sqllogictest rows.
pub fn rows_from_batches(
    batches: &[RecordBatch],
    types: &[DefaultColumnType],
) -> Result<Vec<Vec<String>>> {
    let options = FormatOptions::default();
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_columns() != types.len() {
            bail!(
                "Spice returned {} columns, the record declares {}",
                batch.num_columns(),
                types.len()
            );
        }
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            rows.push(
                types
                    .iter()
                    .enumerate()
                    .map(|(col, ty)| {
                        let array: &Arc<dyn Array> = batch.column(col);
                        let text =
                            (!array.is_null(row)).then(|| formatters[col].value(row).to_string());
                        format_value(text.as_deref(), ty)
                    })
                    .collect(),
            );
        }
    }
    Ok(rows)
}

/// Renders a source's text answer as sqllogictest rows.
pub fn rows_from_text(text: &TextResult, types: &[DefaultColumnType]) -> Result<Vec<Vec<String>>> {
    if text.columns.len() != types.len() {
        bail!(
            "the source returned {} columns, the record declares {}",
            text.columns.len(),
            types.len()
        );
    }
    Ok(text
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(types)
                .map(|(v, ty)| format_value(v.as_deref(), ty))
                .collect()
        })
        .collect())
}

/// Whether `rows` are the answer the record expects, after its sort mode and,
/// when the record holds a hash, after hashing.
pub fn matches(query: &SltQuery, mut rows: Vec<Vec<String>>) -> bool {
    let values: Vec<String> = match query.sort {
        SortMode::NoSort => rows.into_iter().flatten().collect(),
        SortMode::RowSort => {
            rows.sort_unstable();
            rows.into_iter().flatten().collect()
        }
        SortMode::ValueSort => {
            let mut values: Vec<String> = rows.into_iter().flatten().collect();
            values.sort_unstable();
            values
        }
    };
    if let [only] = query.expected.as_slice()
        && let Some((count, hash)) = only.split_once(" values hashing to ")
    {
        let mut md5 = Md5::new();
        for value in &values {
            md5.update(value.as_bytes());
            md5.update(b"\n");
        }
        let digest = format!("{:x}", md5.finalize());
        return count == values.len().to_string() && hash == digest;
    }
    // One value per line (SQLite's layout), or several per line separated by
    // spaces (DataFusion's).
    if query.expected.len() == values.len() {
        return query.expected == values;
    }
    let split: Vec<&str> = query
        .expected
        .iter()
        .flat_map(|line| line.split_whitespace())
        .collect();
    split.len() == values.len() && split.iter().zip(&values).all(|(a, b)| a == b)
}

/// How a query whose answer differs from the recorded one is classified.
pub enum Outcome {
    /// The source gives the recorded answer and Spice does not: Spice's.
    SpiceWrong { got: String },
    /// Spice failed where the source gives the recorded answer.
    SpiceError { error: String },
    /// The source does not give the recorded answer either: dialect.
    Dialect { got: String },
}

pub fn summarize(rows: &[Vec<String>]) -> String {
    let shown: Vec<String> = rows.iter().take(5).map(|r| r.join(" ")).collect();
    if rows.len() > 5 {
        format!("[{}] … {} rows", shown.join(" | "), rows.len())
    } else {
        format!("[{}]", shown.join(" | "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(sort: SortMode, expected: &[&str], types: &str) -> SltQuery {
        SltQuery {
            loc: "t:1".into(),
            sql: String::new(),
            types: types
                .chars()
                .map(|c| match c {
                    'I' => DefaultColumnType::Integer,
                    'R' => DefaultColumnType::FloatingPoint,
                    _ => DefaultColumnType::Text,
                })
                .collect(),
            sort,
            expected: expected.iter().map(|s| (*s).to_string()).collect(),
            expect_error: false,
        }
    }

    #[test]
    fn formats_values_by_declared_type() {
        assert_eq!(format_value(None, &DefaultColumnType::Integer), "NULL");
        assert_eq!(
            format_value(Some("-7.9"), &DefaultColumnType::Integer),
            "-7"
        );
        assert_eq!(format_value(Some("true"), &DefaultColumnType::Integer), "1");
        assert_eq!(
            format_value(Some("2.5"), &DefaultColumnType::FloatingPoint),
            "2.500"
        );
        assert_eq!(
            format_value(Some("104"), &DefaultColumnType::FloatingPoint),
            "104.000"
        );
        assert_eq!(format_value(Some(""), &DefaultColumnType::Text), "(empty)");
    }

    #[test]
    fn sorts_and_hashes_like_sqllogictest() {
        let rows = vec![
            vec!["2".to_string(), "b".into()],
            vec!["1".into(), "a".into()],
        ];
        assert!(matches(
            &query(SortMode::RowSort, &["1", "a", "2", "b"], "IT"),
            rows.clone()
        ));
        assert!(!matches(
            &query(SortMode::NoSort, &["1", "a", "2", "b"], "IT"),
            rows.clone()
        ));
        assert!(matches(
            &query(SortMode::ValueSort, &["1", "2", "a", "b"], "IT"),
            rows.clone()
        ));
        assert!(matches(
            &query(SortMode::RowSort, &["1 a", "2 b"], "IT"),
            rows.clone()
        ));
        // md5("1\na\n2\nb\n")
        let hash = format!("{:x}", Md5::digest(b"1\na\n2\nb\n"));
        let line = format!("4 values hashing to {hash}");
        assert!(matches(
            &query(SortMode::RowSort, &[&line], "IT"),
            rows.clone()
        ));
        assert!(!matches(&query(SortMode::NoSort, &[&line], "IT"), rows));
    }
}
