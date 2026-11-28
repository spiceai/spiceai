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

//! Random predicates for ternary logic partitioning (TLP): for any predicate
//! `p`, the rows of a table are exactly the rows `WHERE p`, `WHERE NOT (p)`
//! and `WHERE (p) IS NULL` together, and its aggregates are the aggregates of
//! those three parts. A query engine that pushes the parts down, or evaluates
//! NULL differently in one of them, breaks the identity.
//!
//! Predicates use only SQL that `DataFusion` and `PostgreSQL` agree on: no
//! ordering comparison of text (collation), no integer division.

use crate::generate::SplitMix64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Int,
    Decimal,
    Float,
    Text,
    Date,
    Timestamp,
    Bool,
}

impl Family {
    /// The family of an Arrow type as `information_schema` prints it.
    pub fn of(data_type: &str) -> Option<Self> {
        let t = data_type;
        Some(if t.starts_with("Int") || t.starts_with("UInt") {
            Self::Int
        } else if t.starts_with("Decimal") {
            Self::Decimal
        } else if t.starts_with("Float") {
            Self::Float
        } else if t.contains("Utf8") {
            Self::Text
        } else if t.starts_with("Date") {
            Self::Date
        } else if t.starts_with("Timestamp") {
            Self::Timestamp
        } else if t == "Boolean" {
            Self::Bool
        } else {
            return None;
        })
    }

    fn numeric(self) -> bool {
        matches!(self, Self::Int | Self::Decimal | Self::Float)
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub family: Family,
}

const WORDS: &[&str] = &[
    "apple",
    "Apple",
    "banana",
    "O''Brien",
    "",
    "b",
    "naïve",
    "under_score",
];
const PATTERNS: &[&str] = &["a%", "%a%", "%e", "_", "O%", "%n_%", ""];

/// A known defect the generated SQL can steer clear of, so a run finds others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Defect {
    /// `(NOT x) IS NULL` is unparsed without its parentheses: write
    /// `x = false`, the same under three-valued logic.
    Not,
    /// A grouping none of whose aggregates is read is unparsed without its
    /// aggregation: read one of every grouping.
    UnreadGroups,
    /// `DuckDB` before 1.5 loses rows of a `UNION ALL` nested in another.
    NestedUnion,
    /// Cayenne cannot filter its files on arithmetic between a decimal and an
    /// integer: write `c * 2.0` for a decimal `c`.
    DecimalInt,
    /// `DataFusion` treats one side's primary key as determining a join's
    /// other columns, and drops them from a GROUP BY over a DISTINCT of the
    /// join: no grouping or DISTINCT over a join.
    JoinKeyDependency,
    /// `INTERSECT` and `EXCEPT` are sent with `=` where they need a NULL-safe
    /// comparison: no set operations.
    SetOperationNulls,
    /// A `CASE` whose first branch is narrower than a later one fails where
    /// `DataFusion` runs it: no `CASE` in computed columns.
    CaseWidening,
    /// `rank()` and `row_number()` are unsigned, so `-rank()` fails: cast
    /// them to `BIGINT`.
    UnsignedRank,
}

impl Defect {
    /// The defect's name in `avoid:` lists and expected-failure labels.
    pub fn name(self) -> &'static str {
        Self::NAMES
            .iter()
            .find(|(_, d)| *d == self)
            .map_or("unknown", |(n, _)| *n)
    }

    pub const NAMES: &[(&str, Defect)] = &[
        ("not", Self::Not),
        ("unread-groups", Self::UnreadGroups),
        ("nested-union", Self::NestedUnion),
        ("decimal-int", Self::DecimalInt),
        ("join-key-dependency", Self::JoinKeyDependency),
        ("set-operation-nulls", Self::SetOperationNulls),
        ("case-widening", Self::CaseWidening),
        ("unsigned-rank", Self::UnsignedRank),
    ];
}

/// The defects a generator steers clear of.
#[derive(Debug, Clone, Default)]
pub struct Avoid(pub Vec<Defect>);

impl Avoid {
    pub fn has(&self, defect: Defect) -> bool {
        self.0.contains(&defect)
    }
}

pub struct Generator<'a> {
    rng: SplitMix64,
    columns: &'a [Column],
    avoid: Avoid,
    /// The known defects whose shapes the current query contains: the labels
    /// a failure of it is recorded under.
    touched: std::collections::BTreeSet<&'static str>,
}

impl<'a> Generator<'a> {
    pub fn new(seed: u64, columns: &'a [Column], avoid: Avoid) -> Self {
        Self {
            rng: SplitMix64(seed),
            columns,
            avoid,
            touched: std::collections::BTreeSet::new(),
        }
    }

    /// Notes that the query takes a shape of a known defect it is not avoiding.
    fn touch(&mut self, defect: Defect) {
        self.touched.insert(defect.name());
    }

    /// The defect shapes of the query generated since the last call.
    pub fn take_touched(&mut self) -> Vec<String> {
        std::mem::take(&mut self.touched)
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    fn not(&mut self, x: &str) -> String {
        if self.avoid.has(Defect::Not) {
            format!("(({x}) = false)")
        } else {
            self.touch(Defect::Not);
            format!("(NOT {x})")
        }
    }

    fn pick<'b, T>(&mut self, items: &'b [T]) -> &'b T {
        let last = i64::try_from(items.len()).unwrap_or(i64::MAX) - 1;
        let index = usize::try_from(self.rng.int(0, last)).unwrap_or(0);
        &items[index]
    }

    fn chance(&mut self, p: f64) -> bool {
        self.rng.unit() < p
    }

    /// A predicate nested up to `depth` boolean operators deep.
    pub fn predicate(&mut self, depth: u32) -> String {
        if depth == 0 || self.chance(0.35) {
            return self.atom();
        }
        match self.rng.int(0, 4) {
            0 => format!(
                "({} AND {})",
                self.predicate(depth - 1),
                self.predicate(depth - 1)
            ),
            1 => format!(
                "({} OR {})",
                self.predicate(depth - 1),
                self.predicate(depth - 1)
            ),
            2 => {
                let inner = self.predicate(depth - 1);
                self.not(&inner)
            }
            3 => format!("(({}) IS NULL)", self.predicate(depth - 1)),
            _ => format!("(({}) IS NOT TRUE)", self.predicate(depth - 1)),
        }
    }

    fn numeric_literal(&mut self, family: Family) -> String {
        match family {
            Family::Int => self.rng.int(-60, 60).to_string(),
            Family::Decimal => format!("{}.{:02}", self.rng.int(-100, 100), self.rng.int(0, 99)),
            _ => format!("{}.{}", self.rng.int(-5, 55), self.rng.int(0, 9)),
        }
    }

    fn numeric_expr(&mut self, column: &Column) -> String {
        let c = &column.name;
        match self.rng.int(0, 5) {
            0 => format!("abs({c})"),
            1 => format!("({c} + {})", self.numeric_literal(column.family)),
            2 if self.avoid.has(Defect::DecimalInt) && column.family == Family::Decimal => {
                format!("({c} * 2.0)")
            }
            2 => {
                if column.family == Family::Decimal {
                    self.touch(Defect::DecimalInt);
                }
                format!("({c} * 2)")
            }
            3 => format!("coalesce({c}, {})", self.numeric_literal(column.family)),
            4 => format!("(- {c})"),
            _ => c.clone(),
        }
    }

    fn atom(&mut self) -> String {
        if self.chance(0.03) {
            return (*self.pick(&["NULL", "TRUE", "FALSE", "(1 = NULL)"])).to_string();
        }
        let column = self.pick(self.columns).clone();
        let col = column.name.as_str();
        if self.chance(0.12) {
            return format!(
                "({col} IS {}NULL)",
                if self.chance(0.5) { "NOT " } else { "" }
            );
        }
        match column.family {
            f if f.numeric() => {
                let e = self.numeric_expr(&column);
                match self.rng.int(0, 5) {
                    0 => {
                        let (a, b) = (self.numeric_literal(f), self.numeric_literal(f));
                        format!("({e} BETWEEN {a} AND {b})")
                    }
                    1 => {
                        let mut list: Vec<String> =
                            (0..3).map(|_| self.numeric_literal(f)).collect();
                        if self.chance(0.3) {
                            list.push("NULL".into());
                        }
                        let not = if self.chance(0.4) { "NOT " } else { "" };
                        format!("({e} {not}IN ({}))", list.join(", "))
                    }
                    2 => {
                        let other: Vec<Column> = self
                            .columns
                            .iter()
                            .filter(|o| o.family.numeric())
                            .cloned()
                            .collect();
                        let o = self.pick(&other).name.clone();
                        let op = *self.pick(&["=", "<>", "<", "<=", ">", ">="]);
                        format!("({e} {op} {o})")
                    }
                    _ => {
                        let op = *self.pick(&["=", "<>", "<", "<=", ">", ">="]);
                        format!("({e} {op} {})", self.numeric_literal(f))
                    }
                }
            }
            Family::Text => match self.rng.int(0, 5) {
                0 => format!("({col} LIKE '{}')", self.pick(PATTERNS)),
                1 => format!(
                    "(lower({col}) = '{}')",
                    self.pick(&["apple", "banana", "b", ""])
                ),
                2 => format!("(char_length({col}) > {})", self.rng.int(0, 6)),
                3 => {
                    let list: Vec<String> =
                        (0..3).map(|_| format!("'{}'", self.pick(WORDS))).collect();
                    format!("({col} IN ({}))", list.join(", "))
                }
                4 => format!("({col} <> '{}')", self.pick(WORDS)),
                _ => format!("({col} = '{}')", self.pick(WORDS)),
            },
            Family::Date => {
                let d = format!(
                    "DATE '2024-{:02}-{:02}'",
                    self.rng.int(1, 12),
                    self.rng.int(1, 28)
                );
                let op = *self.pick(&["=", "<", "<=", ">", ">=", "<>"]);
                format!("({col} {op} {d})")
            }
            Family::Timestamp => {
                let t = format!(
                    "TIMESTAMP '2024-{:02}-{:02} {:02}:00:00'",
                    self.rng.int(1, 12),
                    self.rng.int(1, 28),
                    self.rng.int(0, 23)
                );
                let op = *self.pick(&["<", "<=", ">", ">="]);
                format!("({col} {op} {t})")
            }
            Family::Bool => match self.rng.int(0, 3) {
                0 => format!("({col})"),
                1 => self.not(col),
                2 => format!(
                    "({col} = {})",
                    if self.chance(0.5) { "true" } else { "false" }
                ),
                _ => format!("({col} IS TRUE)"),
            },
            _ => format!("({col} IS NULL)"),
        }
    }
}
