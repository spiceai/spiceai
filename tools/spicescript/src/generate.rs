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

//! Seeded table generator.
//!
//! Rows come from a self-contained `SplitMix64` stream per column, seeded from
//! the table's seed and the column's name, so the same spec produces the same
//! rows on every instance and with every dependency version, and adding a
//! column leaves the others' values unchanged. Values are produced as text and
//! loaded by the source in bulk.

use anyhow::{Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct TableSpec {
    pub table: String,
    pub rows: u64,
    #[serde(default)]
    pub seed: u64,
    /// A list, so the table's columns keep the order they are written in.
    pub columns: Vec<NamedColumn>,
    #[serde(default)]
    pub primary_key: Option<String>,
    /// For a `files` source: the file format (default parquet).
    #[serde(default)]
    pub format: FileFormat,
    /// For a `files` source: how many part files to split the rows across.
    #[serde(default = "one")]
    pub files: u32,
    /// `replace` (default) recreates the table or its files; `append` adds the
    /// rows to what is there (for a database, the table must exist).
    #[serde(default)]
    pub mode: WriteMode,
}

fn one() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileFormat {
    #[default]
    Parquet,
    Csv,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WriteMode {
    #[default]
    Replace,
    Append,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamedColumn {
    pub name: String,
    #[serde(flatten)]
    pub spec: ColumnSpec,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ColumnSpec {
    #[serde(flatten)]
    pub kind: ColumnKind,
    /// Probability that a value is NULL.
    #[serde(default)]
    pub nulls: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ColumnKind {
    /// `start`, `start + 1`, … — unique, for keys.
    Sequence(i64),
    /// Uniform in `[lo, hi]`, as a 32-bit integer.
    Int([i64; 2]),
    /// Uniform in `[lo, hi]`, as a 64-bit integer.
    Bigint([i64; 2]),
    Float([f64; 2]),
    /// Uniform in `[lo, hi]`, rounded to `scale` digits (default 2).
    Decimal(DecimalSpec),
    /// One of these strings, uniformly.
    Choice(Vec<String>),
    /// Lowercase ASCII letters, length uniform in `[min, max]`.
    Text([usize; 2]),
    Bool(f64),
    /// Uniform between two dates (`YYYY-MM-DD`).
    Date([String; 2]),
    /// Uniform between two instants, whole microseconds.
    Timestamp([String; 2]),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DecimalSpec {
    Range([f64; 2]),
    Scaled { range: [f64; 2], scale: u32 },
}

/// The SQL dialect a table is created in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Mysql,
    ClickHouse,
}

impl ColumnKind {
    /// The `ClickHouse` type: full-range dates and microsecond timestamps.
    fn clickhouse_type(&self) -> String {
        match self {
            Self::Sequence(_) | Self::Bigint(_) => "Int64".into(),
            Self::Int(_) => "Int32".into(),
            Self::Float(_) => "Float64".into(),
            Self::Decimal(d) => format!("Decimal(18, {})", d.scale()),
            Self::Choice(_) | Self::Text(_) => "String".into(),
            Self::Bool(_) => "Bool".into(),
            Self::Date(_) => "Date32".into(),
            Self::Timestamp(_) => "DateTime64(6)".into(),
        }
    }

    fn sql_type(&self, dialect: Dialect) -> String {
        if dialect == Dialect::ClickHouse {
            return self.clickhouse_type();
        }
        let pg = dialect == Dialect::Postgres;
        match self {
            Self::Sequence(_) | Self::Bigint(_) => "BIGINT".into(),
            Self::Int(_) => "INTEGER".into(),
            Self::Float(_) => if pg { "DOUBLE PRECISION" } else { "DOUBLE" }.into(),
            Self::Decimal(d) => format!("DECIMAL(18, {})", d.scale()),
            Self::Choice(_) => "VARCHAR(64)".into(),
            Self::Text([_, max]) => format!("VARCHAR({})", (*max).max(1)),
            Self::Bool(_) => "BOOLEAN".into(),
            Self::Date(_) => "DATE".into(),
            Self::Timestamp(_) => if pg { "TIMESTAMP" } else { "DATETIME(6)" }.into(),
        }
    }
}

impl DecimalSpec {
    fn range(&self) -> [f64; 2] {
        match self {
            Self::Range(r) | Self::Scaled { range: r, .. } => *r,
        }
    }
    fn scale(&self) -> u32 {
        match self {
            Self::Range(_) => 2,
            Self::Scaled { scale, .. } => *scale,
        }
    }
}

impl ColumnKind {
    /// The Arrow type a column is written to files with.
    pub fn arrow_type(&self) -> arrow::datatypes::DataType {
        use arrow::datatypes::{DataType, TimeUnit};
        match self {
            Self::Sequence(_) | Self::Bigint(_) => DataType::Int64,
            Self::Int(_) => DataType::Int32,
            Self::Float(_) => DataType::Float64,
            #[expect(clippy::cast_possible_truncation, reason = "scales are small")]
            Self::Decimal(d) => DataType::Decimal128(18, d.scale() as i8),
            Self::Choice(_) | Self::Text(_) => DataType::Utf8,
            Self::Bool(_) => DataType::Boolean,
            Self::Date(_) => DataType::Date32,
            Self::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }
}

/// Typed Arrow columns for rows of text cells.
pub fn to_batch(
    columns: &[NamedColumn],
    rows: &[Vec<Option<String>>],
) -> Result<arrow::array::RecordBatch> {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, StringArray},
        compute::{CastOptions, cast_with_options},
        datatypes::{Field, Schema},
        util::display::FormatOptions,
    };
    let strict = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (i, column) in columns.iter().enumerate() {
        let data_type = column.spec.kind.arrow_type();
        let text: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|r| r[i].as_deref())));
        arrays.push(cast_with_options(&text, &data_type, &strict)?);
        fields.push(Field::new(&column.name, data_type, true));
    }
    Ok(arrow::array::RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        arrays,
    )?)
}

/// `CREATE TABLE` for the spec in the dialect.
pub fn create_table(spec: &TableSpec, dialect: Dialect) -> String {
    if dialect == Dialect::ClickHouse {
        // Columns are non-nullable unless declared otherwise; the sort key
        // stands in for a primary key.
        let columns: Vec<String> = spec
            .columns
            .iter()
            .map(|c| {
                let t = c.spec.kind.clickhouse_type();
                if c.spec.nulls > 0.0 {
                    format!("{} Nullable({t})", c.name)
                } else {
                    format!("{} {t}", c.name)
                }
            })
            .collect();
        let order = spec.primary_key.as_deref().unwrap_or("tuple()");
        return format!(
            "CREATE TABLE {} ({}) ENGINE = MergeTree ORDER BY ({order})",
            spec.table,
            columns.join(", ")
        );
    }
    let mut columns: Vec<String> = spec
        .columns
        .iter()
        .map(|c| format!("{} {}", c.name, c.spec.kind.sql_type(dialect)))
        .collect();
    if let Some(pk) = &spec.primary_key {
        columns.push(format!("PRIMARY KEY ({pk})"));
    }
    format!("CREATE TABLE {} ({})", spec.table, columns.join(", "))
}

/// `SplitMix64`: tiny, fast and fully determined by its seed.
pub(crate) struct SplitMix64(pub(crate) u64);

impl SplitMix64 {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    #[expect(
        clippy::cast_precision_loss,
        reason = "53 random bits fit an f64 exactly"
    )]
    pub(crate) fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `[lo, hi]`.
    #[expect(
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        reason = "span arithmetic in u64"
    )]
    pub(crate) fn int(&mut self, lo: i64, hi: i64) -> i64 {
        let span = hi.wrapping_sub(lo) as u64 + 1;
        if span == 0 {
            return self.next() as i64;
        }
        lo.wrapping_add((self.next() % span) as i64)
    }
}

fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Generates the rows of `spec` as text cells, `None` for NULL.
pub struct Rows {
    columns: Vec<(ColumnSpec, SplitMix64, Bounds)>,
    next: u64,
    total: u64,
}

enum Bounds {
    None,
    Days(i64, i64),
    Micros(i64, i64),
}

pub fn rows(spec: &TableSpec) -> Result<Rows> {
    let mut columns = Vec::new();
    for NamedColumn { name, spec: column } in &spec.columns {
        if !(0.0..=1.0).contains(&column.nulls) {
            bail!("column '{name}': `nulls` must be between 0 and 1");
        }
        let bounds = match &column.kind {
            ColumnKind::Date([lo, hi]) => Bounds::Days(days(lo)?, days(hi)?),
            ColumnKind::Timestamp([lo, hi]) => Bounds::Micros(micros(lo)?, micros(hi)?),
            ColumnKind::Choice(c) if c.is_empty() => bail!("column '{name}': `choice` is empty"),
            _ => Bounds::None,
        };
        let seed = spec.seed ^ fnv1a(name);
        columns.push((column.clone(), SplitMix64(seed), bounds));
    }
    Ok(Rows {
        columns,
        next: 0,
        total: spec.rows,
    })
}

impl Iterator for Rows {
    type Item = Vec<Option<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.total {
            return None;
        }
        let row = self.next;
        self.next += 1;
        Some(
            self.columns
                .iter_mut()
                .map(|(spec, rng, bounds)| value(spec, rng, bounds, row))
                .collect(),
        )
    }
}

#[expect(
    clippy::cast_possible_wrap,
    reason = "row counts stay far below i64::MAX"
)]
fn value(spec: &ColumnSpec, rng: &mut SplitMix64, bounds: &Bounds, row: u64) -> Option<String> {
    // Always draw, so whether a value is NULL never shifts later values.
    let null = rng.unit() < spec.nulls;
    let text = match &spec.kind {
        ColumnKind::Sequence(start) => return Some((start + row as i64).to_string()),
        ColumnKind::Int([lo, hi]) | ColumnKind::Bigint([lo, hi]) => rng.int(*lo, *hi).to_string(),
        ColumnKind::Float([lo, hi]) => format!("{}", lo + rng.unit() * (hi - lo)),
        ColumnKind::Decimal(d) => {
            let [lo, hi] = d.range();
            let scale = d.scale() as usize;
            format!("{:.scale$}", lo + rng.unit() * (hi - lo))
        }
        ColumnKind::Choice(options) => {
            #[expect(clippy::cast_possible_truncation, reason = "index into a small list")]
            let i = (rng.next() % options.len() as u64) as usize;
            options[i].clone()
        }
        ColumnKind::Text([min, max]) => {
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "small lengths"
            )]
            let len = rng.int(*min as i64, *max as i64) as usize;
            (0..len)
                .map(|_| char::from(b'a' + u8::try_from(rng.next() % 26).unwrap_or(0)))
                .collect()
        }
        ColumnKind::Bool(p) => (rng.unit() < *p).to_string(),
        ColumnKind::Date(_) | ColumnKind::Timestamp(_) => match bounds {
            Bounds::Days(lo, hi) => date_text(rng.int(*lo, *hi)),
            Bounds::Micros(lo, hi) => timestamp_text(rng.int(*lo, *hi)),
            Bounds::None => unreachable!("dates and timestamps always have bounds"),
        },
    };
    (!null).then_some(text)
}

fn days(date: &str) -> Result<i64> {
    let d = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")?;
    Ok(d.signed_duration_since(epoch_date()).num_days())
}

fn micros(ts: &str) -> Result<i64> {
    let parsed = chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S").or_else(|_| {
        chrono::NaiveDate::parse_from_str(ts, "%Y-%m-%d")
            .map(|d| d.and_time(chrono::NaiveTime::MIN))
    })?;
    Ok(parsed.and_utc().timestamp_micros())
}

fn epoch_date() -> chrono::NaiveDate {
    chrono::DateTime::UNIX_EPOCH.date_naive()
}

fn date_text(days: i64) -> String {
    (epoch_date() + chrono::Duration::days(days))
        .format("%Y-%m-%d")
        .to_string()
}

fn timestamp_text(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|t| t.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(yaml: &str) -> TableSpec {
        yaml::from_str(yaml).expect("spec parses")
    }

    #[test]
    fn same_seed_same_rows_and_columns_are_independent() {
        let a = spec(
            "table: t\nrows: 50\nseed: 7\ncolumns: [{ name: id, sequence: 1 }, { name: v, int: [1, 10] }, { name: s, text: [2, 5], nulls: 0.3 }]",
        );
        let b = spec(
            "table: t\nrows: 50\nseed: 7\ncolumns: [{ name: id, sequence: 1 }, { name: v, int: [1, 10] }, { name: s, text: [2, 5], nulls: 0.3 }, { name: extra, float: [0, 1] }]",
        );
        let rows_a: Vec<_> = rows(&a).expect("rows").collect();
        let rows_b: Vec<_> = rows(&b).expect("rows").collect();
        assert_eq!(rows_a.len(), 50);
        for (x, y) in rows_a.iter().zip(&rows_b) {
            assert_eq!(x[..], y[..3], "adding a column must not change the others");
        }
        assert_eq!(rows_a[0][0].as_deref(), Some("1"));
        assert_eq!(rows_a[49][0].as_deref(), Some("50"));
        assert!(rows_a.iter().all(|r| {
            r[1].as_ref()
                .is_some_and(|v| (1..=10).contains(&v.parse::<i64>().unwrap_or(0)))
        }));
        assert!(rows_a.iter().any(|r| r[2].is_none()) && rows_a.iter().any(|r| r[2].is_some()));
    }

    #[test]
    fn renders_dates_timestamps_and_ddl() {
        let s = spec(
            "table: t\nrows: 3\nseed: 1\nprimary-key: id\ncolumns: [{ name: id, sequence: 1 }, { name: d, date: ['2024-01-01', '2024-01-01'] }, { name: ts, timestamp: ['2024-01-02 03:04:05', '2024-01-02 03:04:05'] }, { name: m, decimal: { range: [1, 1], scale: 3 } }]",
        );
        let first = rows(&s).expect("rows").next().expect("a row");
        assert_eq!(
            first,
            vec![
                Some("1".to_string()),
                Some("2024-01-01".into()),
                Some("2024-01-02 03:04:05.000000".into()),
                Some("1.000".into())
            ]
        );
        assert_eq!(
            create_table(&s, Dialect::Mysql),
            "CREATE TABLE t (id BIGINT, d DATE, ts DATETIME(6), m DECIMAL(18, 3), PRIMARY KEY (id))"
        );
    }
}
