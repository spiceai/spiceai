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

//! Random query generation: queries built from nested relations (filters,
//! groupings, `DISTINCT`, `ORDER BY … LIMIT` on a unique key, joins and
//! `UNION ALL`) under an outer query that reads some, none or all of their
//! columns. The outer query is what lets the optimizer prune a subquery's
//! outputs, which is where an engine that rewrites a plan before sending it
//! elsewhere can lose part of it. Each query's answer is compared with an
//! oracle that answers the same SQL.
//!
//! The SQL keeps to what `DataFusion` and `PostgreSQL` agree on: no ordering
//! or `min`/`max` of text (collation), no integer division, no `avg` (its
//! type differs), and a `LIMIT` only under an `ORDER BY` of a unique key with
//! the NULL placement stated. `Avoid` steers clear of known defects so a run
//! can find others.

use crate::generate::SplitMix64;
use crate::tlp::{Avoid, Column, Defect, Family, Generator as Predicates};

/// A table the queries read, with the column whose values are unique in it.
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub key: Option<String>,
}

/// An equality two tables may be joined on: `left.left_column = right.right_column`.
#[derive(Debug, Clone)]
pub struct Join {
    pub left: String,
    pub left_column: String,
    pub right: String,
    pub right_column: String,
}

/// A relation a query reads: SQL usable in a `FROM` clause, its columns, and
/// the columns (if any) whose values together are unique in it.
#[derive(Debug, Clone)]
struct Relation {
    from: String,
    columns: Vec<Column>,
    unique: Option<Vec<String>>,
    /// Columns every consumer reads, so a grouping below always has an
    /// aggregate in use (when avoiding `Defect::UnreadGroups`).
    must_read: Vec<String>,
    /// Whether a `UNION ALL` is anywhere beneath.
    has_union: bool,
    /// Whether a join is anywhere beneath.
    has_join: bool,
}

pub struct Generator<'a> {
    rng: SplitMix64,
    tables: &'a [Table],
    joins: &'a [Join],
    avoid: Avoid,
    next_name: usize,
    /// The known defects whose shapes the current query contains.
    touched: std::collections::BTreeSet<String>,
}

impl<'a> Generator<'a> {
    pub fn new(seed: u64, tables: &'a [Table], joins: &'a [Join], avoid: Avoid) -> Self {
        Self {
            rng: SplitMix64(seed),
            tables,
            joins,
            avoid,
            next_name: 0,
            touched: std::collections::BTreeSet::new(),
        }
    }

    /// Notes that the query takes a shape of a known defect it is not avoiding.
    fn touch(&mut self, defect: Defect) {
        self.touched.insert(defect.name().to_string());
    }

    /// The defect shapes of the query generated since the last call.
    pub fn take_touched(&mut self) -> Vec<String> {
        std::mem::take(&mut self.touched).into_iter().collect()
    }

    fn index(&mut self, len: usize) -> usize {
        let last = i64::try_from(len).unwrap_or(i64::MAX) - 1;
        usize::try_from(self.rng.int(0, last)).unwrap_or(0)
    }

    fn chance(&mut self, p: f64) -> bool {
        self.rng.unit() < p
    }

    fn fresh(&mut self, prefix: &str) -> String {
        self.next_name += 1;
        format!("{prefix}{}", self.next_name)
    }

    fn predicate(&mut self, columns: &[Column], depth: u32) -> String {
        let seed = self.rng.int(0, i64::MAX).unsigned_abs();
        let mut predicates = Predicates::new(seed, columns, self.avoid.clone());
        let predicate = predicates.predicate(depth);
        self.touched.extend(predicates.take_touched());
        predicate
    }

    /// A subset of the child's columns that keeps every one it must read.
    fn subset_reading(&mut self, child: &Relation) -> Vec<Column> {
        let mut keep = self.subset(&child.columns);
        for name in &child.must_read {
            if !keep.iter().any(|c| &c.name == name)
                && let Some(c) = child.columns.iter().find(|c| &c.name == name)
            {
                keep.push(c.clone());
            }
        }
        keep
    }

    /// The new names of `names` under a renaming.
    fn mapped(names: &[String], map: &[(String, String)]) -> Vec<String> {
        names
            .iter()
            .filter_map(|n| {
                map.iter()
                    .find(|(from, _)| from == n)
                    .map(|(_, to)| to.clone())
            })
            .collect()
    }

    /// A non-empty subset of `columns`, in their order.
    fn subset(&mut self, columns: &[Column]) -> Vec<Column> {
        let mut picked: Vec<Column> = columns
            .iter()
            .filter(|_| self.chance(0.5))
            .cloned()
            .collect();
        if picked.is_empty() {
            picked.push(columns[self.index(columns.len())].clone());
        }
        picked
    }

    /// `SELECT` items renaming each of `columns` to a fresh name.
    fn renamed(&mut self, columns: &[Column]) -> (String, Vec<Column>, Vec<(String, String)>) {
        let mut items = Vec::new();
        let mut out = Vec::new();
        let mut map = Vec::new();
        for c in columns {
            let name = self.fresh("c");
            items.push(format!("{} AS {name}", c.name));
            map.push((c.name.clone(), name.clone()));
            out.push(Column {
                name,
                family: c.family,
            });
        }
        (items.join(", "), out, map)
    }

    fn base(&mut self) -> Relation {
        let table = self.tables[self.index(self.tables.len())].clone();
        let alias = self.fresh("r");
        let (items, columns, map) = self.renamed(&table.columns);
        let unique = table.key.as_ref().and_then(|k| {
            map.iter()
                .find(|(from, _)| from == k)
                .map(|(_, to)| vec![to.clone()])
        });
        Relation {
            from: format!("(SELECT {items} FROM {}) AS {alias}", table.name),
            columns,
            unique,
            must_read: Vec::new(),
            has_union: false,
            has_join: false,
        }
    }

    fn relation(&mut self, depth: u32) -> Relation {
        if depth == 0 || self.chance(0.2) {
            return self.base();
        }
        match self.rng.int(0, 11) {
            0..=2 => self.filtered(depth),
            3..=5 => self.grouped(depth),
            6 => self.distinct(depth),
            7 => self.limited(depth),
            8 if !self.joins.is_empty() => self.joined(),
            9 => self.windowed(depth),
            10 if !self.avoid.has(Defect::SetOperationNulls) => self.set_operation(depth),
            _ => self.unioned(depth),
        }
    }

    fn filtered(&mut self, depth: u32) -> Relation {
        let child = self.relation(depth - 1);
        let keep = self.subset_reading(&child);
        let predicate = self.predicate(&child.columns, 2);
        let alias = self.fresh("r");
        let (mut items, mut columns, map) = self.renamed(&keep);
        let numeric: Vec<Column> = child
            .columns
            .iter()
            .filter(|c| matches!(c.family, Family::Int | Family::Decimal | Family::Float))
            .cloned()
            .collect();
        if !numeric.is_empty() && self.chance(0.4) {
            let (a, b) = (
                numeric[self.index(numeric.len())].clone(),
                numeric[self.index(numeric.len())].clone(),
            );
            let name = self.fresh("c");
            let condition = self.predicate(&child.columns, 1);
            let first = i64::from(self.avoid.has(Defect::CaseWidening));
            let expr = match self.rng.int(first, 2) {
                0 => {
                    self.touch(Defect::CaseWidening);
                    format!("CASE WHEN {condition} THEN {} ELSE {} END", a.name, b.name)
                }
                1 => format!("coalesce({}, {})", a.name, b.name),
                _ => format!("({} + {})", a.name, b.name),
            };
            // Mixed families widen; a decimal with a float is a float.
            let family = match (a.family, b.family) {
                (x, y) if x == y => x,
                (Family::Float, _) | (_, Family::Float) => Family::Float,
                _ => Family::Decimal,
            };
            items = format!("{items}, {expr} AS {name}");
            columns.push(Column { name, family });
        }
        let unique = child.unique.as_ref().and_then(|u| {
            u.iter()
                .map(|k| {
                    map.iter()
                        .find(|(from, _)| from == k)
                        .map(|(_, to)| to.clone())
                })
                .collect::<Option<Vec<_>>>()
        });
        Relation {
            from: format!(
                "(SELECT {items} FROM {} WHERE {predicate}) AS {alias}",
                child.from
            ),
            columns,
            unique,
            must_read: Self::mapped(&child.must_read, &map),
            has_union: child.has_union,
            has_join: child.has_join,
        }
    }

    fn grouped(&mut self, depth: u32) -> Relation {
        let child = self.relation(depth - 1);
        if child.has_join {
            if self.avoid.has(Defect::JoinKeyDependency) {
                return child;
            }
            self.touch(Defect::JoinKeyDependency);
        }
        if !self.avoid.has(Defect::UnreadGroups) {
            self.touch(Defect::UnreadGroups);
        }
        let mut keys = vec![child.columns[self.index(child.columns.len())].clone()];
        if self.chance(0.3) {
            let other = child.columns[self.index(child.columns.len())].clone();
            if other.name != keys[0].name {
                keys.push(other);
            }
        }
        let alias = self.fresh("r");
        let mut items = Vec::new();
        let mut columns = Vec::new();
        let mut unique = Vec::new();
        for k in &keys {
            let name = self.fresh("c");
            items.push(format!("{} AS {name}", k.name));
            unique.push(name.clone());
            columns.push(Column {
                name,
                family: k.family,
            });
        }
        let count = self.fresh("c");
        items.push(format!("count(*) AS {count}"));
        columns.push(Column {
            name: count.clone(),
            family: Family::Int,
        });
        for name in &child.must_read {
            let read = self.fresh("c");
            items.push(format!("max({name}) AS {read}"));
            columns.push(Column {
                name: read,
                family: Family::Int,
            });
        }
        for _ in 0..self.rng.int(0, 2) {
            let c = child.columns[self.index(child.columns.len())].clone();
            let name = self.fresh("c");
            let (expr, family) = match c.family {
                Family::Int | Family::Decimal | Family::Float => match self.rng.int(0, 3) {
                    0 => (format!("sum({})", c.name), c.family),
                    1 => (format!("min({})", c.name), c.family),
                    2 => (format!("max({})", c.name), c.family),
                    _ => (format!("count({})", c.name), Family::Int),
                },
                Family::Date | Family::Timestamp if self.chance(0.6) => (
                    format!(
                        "{}({})",
                        if self.chance(0.5) { "min" } else { "max" },
                        c.name
                    ),
                    c.family,
                ),
                _ => {
                    let distinct = if self.chance(0.4) { "DISTINCT " } else { "" };
                    (format!("count({distinct}{})", c.name), Family::Int)
                }
            };
            items.push(format!("{expr} AS {name}"));
            columns.push(Column { name, family });
        }
        let group_by: Vec<&str> = keys.iter().map(|k| k.name.as_str()).collect();
        let having = if self.chance(0.3) {
            format!(" HAVING count(*) > {}", self.rng.int(0, 3))
        } else {
            String::new()
        };
        let filter = if self.chance(0.4) {
            format!(" WHERE {}", self.predicate(&child.columns, 1))
        } else {
            String::new()
        };
        Relation {
            from: format!(
                "(SELECT {} FROM {}{filter} GROUP BY {}{having}) AS {alias}",
                items.join(", "),
                child.from,
                group_by.join(", ")
            ),
            columns,
            unique: Some(unique),
            must_read: if self.avoid.has(Defect::UnreadGroups) {
                vec![count]
            } else {
                Vec::new()
            },
            has_union: child.has_union,
            has_join: child.has_join,
        }
    }

    fn distinct(&mut self, depth: u32) -> Relation {
        let child = self.relation(depth - 1);
        if child.has_join {
            if self.avoid.has(Defect::JoinKeyDependency) {
                return child;
            }
            self.touch(Defect::JoinKeyDependency);
        }
        let keep = self.subset_reading(&child);
        let alias = self.fresh("r");
        let (items, columns, map) = self.renamed(&keep);
        let unique = Some(columns.iter().map(|c| c.name.clone()).collect());
        Relation {
            from: format!("(SELECT DISTINCT {items} FROM {}) AS {alias}", child.from),
            columns,
            unique,
            must_read: Self::mapped(&child.must_read, &map),
            has_union: child.has_union,
            has_join: child.has_join,
        }
    }

    /// A unique key that orders the same in every engine: none of its columns
    /// is text, whose order depends on the collation.
    fn orderable(relation: &Relation) -> Option<Vec<String>> {
        let unique = relation.unique.clone()?;
        unique
            .iter()
            .all(|k| {
                relation
                    .columns
                    .iter()
                    .any(|c| &c.name == k && c.family != Family::Text)
            })
            .then_some(unique)
    }

    fn limited(&mut self, depth: u32) -> Relation {
        let child = self.relation(depth - 1);
        let Some(unique) = Self::orderable(&child) else {
            return self.filtered(depth);
        };
        let alias = self.fresh("r");
        let (items, columns, map) = self.renamed(&child.columns);
        let order: Vec<String> = unique
            .iter()
            .map(|k| format!("{k} ASC NULLS LAST"))
            .collect();
        let offset = if self.chance(0.3) {
            format!(" OFFSET {}", self.rng.int(1, 5))
        } else {
            String::new()
        };
        let unique = unique
            .iter()
            .map(|k| {
                map.iter()
                    .find(|(from, _)| from == k)
                    .map(|(_, to)| to.clone())
            })
            .collect::<Option<Vec<_>>>();
        Relation {
            from: format!(
                "(SELECT {items} FROM {} ORDER BY {} LIMIT {}{offset}) AS {alias}",
                child.from,
                order.join(", "),
                self.rng.int(1, 25)
            ),
            columns,
            unique,
            must_read: Self::mapped(&child.must_read, &map),
            has_union: child.has_union,
            has_join: child.has_join,
        }
    }

    fn joined(&mut self) -> Relation {
        let join = self.joins[self.index(self.joins.len())].clone();
        let find = |name: &str| self.tables.iter().find(|t| t.name == name).cloned();
        let (Some(left), Some(right)) = (find(&join.left), find(&join.right)) else {
            return self.base();
        };
        let kind = ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"][self.index(4)];
        let alias = self.fresh("r");
        let mut items = Vec::new();
        let mut columns = Vec::new();
        for (side, table) in [("a", &left), ("b", &right)] {
            for c in &table.columns {
                if self.chance(0.6) {
                    let name = self.fresh("c");
                    items.push(format!("{side}.{} AS {name}", c.name));
                    columns.push(Column {
                        name,
                        family: c.family,
                    });
                }
            }
        }
        if items.is_empty() {
            let name = self.fresh("c");
            items.push(format!("a.{} AS {name}", join.left_column));
            let family = left
                .columns
                .iter()
                .find(|c| c.name == join.left_column)
                .map_or(Family::Int, |c| c.family);
            columns.push(Column { name, family });
        }
        Relation {
            from: format!(
                "(SELECT {} FROM {} a {kind} {} b ON a.{} = b.{}) AS {alias}",
                items.join(", "),
                left.name,
                right.name,
                join.left_column,
                join.right_column
            ),
            columns,
            unique: None,
            must_read: Vec::new(),
            has_union: false,
            has_join: true,
        }
    }

    /// Window functions over the child, ordered by its unique key so every
    /// engine numbers the rows alike.
    fn windowed(&mut self, depth: u32) -> Relation {
        let child = self.relation(depth - 1);
        let Some(unique) = Self::orderable(&child) else {
            return self.filtered(depth);
        };
        let order: Vec<String> = unique
            .iter()
            .map(|k| format!("{k} ASC NULLS LAST"))
            .collect();
        let order = order.join(", ");
        let alias = self.fresh("r");
        let (mut items, mut columns, map) = self.renamed(&child.columns);
        let partition = child.columns[self.index(child.columns.len())].name.clone();
        let numeric: Vec<Column> = child
            .columns
            .iter()
            .filter(|c| matches!(c.family, Family::Int | Family::Decimal | Family::Float))
            .cloned()
            .collect();
        for _ in 0..self.rng.int(1, 2) {
            let name = self.fresh("c");
            let avoid_rank = self.avoid.has(Defect::UnsignedRank);
            let mut uncast_rank = false;
            let mut rank = |f: String, _: &Avoid| {
                if avoid_rank {
                    format!("CAST({f} AS BIGINT)")
                } else {
                    uncast_rank = true;
                    f
                }
            };
            let (expr, family) = match self.rng.int(0, 3) {
                0 => (
                    rank(format!("row_number() OVER (ORDER BY {order})"), &self.avoid),
                    Family::Int,
                ),
                1 => (
                    rank(
                        format!("rank() OVER (PARTITION BY {partition} ORDER BY {order})"),
                        &self.avoid,
                    ),
                    Family::Int,
                ),
                2 => (
                    format!("count(*) OVER (PARTITION BY {partition})"),
                    Family::Int,
                ),
                _ if !numeric.is_empty() => {
                    let c = &numeric[self.index(numeric.len())];
                    (
                        format!(
                            "sum({}) OVER (PARTITION BY {partition} ORDER BY {order})",
                            c.name
                        ),
                        c.family,
                    )
                }
                _ => (
                    rank(format!("row_number() OVER (ORDER BY {order})"), &self.avoid),
                    Family::Int,
                ),
            };
            if uncast_rank {
                self.touch(Defect::UnsignedRank);
            }
            items = format!("{items}, {expr} AS {name}");
            columns.push(Column { name, family });
        }
        Relation {
            from: format!("(SELECT {items} FROM {}) AS {alias}", child.from),
            columns,
            unique: unique
                .iter()
                .map(|k| {
                    map.iter()
                        .find(|(from, _)| from == k)
                        .map(|(_, to)| to.clone())
                })
                .collect::<Option<Vec<_>>>(),
            must_read: Self::mapped(&child.must_read, &map),
            has_union: child.has_union,
            has_join: child.has_join,
        }
    }

    /// `INTERSECT` or `EXCEPT` of two filtered readings of the child (set
    /// semantics, so every output column together is unique).
    fn set_operation(&mut self, depth: u32) -> Relation {
        self.touch(Defect::SetOperationNulls);
        let child = self.relation(depth - 1);
        let keep = self.subset_reading(&child);
        let alias = self.fresh("r");
        let (items, columns, map) = self.renamed(&keep);
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let (left, right) = (
            self.predicate(&child.columns, 1),
            self.predicate(&child.columns, 1),
        );
        let op = if self.chance(0.5) {
            "INTERSECT"
        } else {
            "EXCEPT"
        };
        let inner = format!(
            "SELECT {items} FROM {from} WHERE {left} {op} SELECT {items} FROM {from} WHERE {right}",
            from = child.from
        );
        Relation {
            from: format!(
                "(SELECT {} FROM ({inner}) AS {alias}s) AS {alias}",
                names.join(", ")
            ),
            unique: Some(columns.iter().map(|c| c.name.clone()).collect()),
            columns,
            must_read: Self::mapped(&child.must_read, &map),
            has_union: child.has_union,
            has_join: child.has_join,
        }
    }

    fn unioned(&mut self, depth: u32) -> Relation {
        let child = self.relation(depth - 1);
        if child.has_union {
            if self.avoid.has(Defect::NestedUnion) {
                return child;
            }
            self.touch(Defect::NestedUnion);
        }
        let keep = self.subset_reading(&child);
        let alias = self.fresh("r");
        let (items, columns, map) = self.renamed(&keep);
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let (left, right) = (
            self.predicate(&child.columns, 1),
            self.predicate(&child.columns, 1),
        );
        let inner = format!(
            "SELECT {items} FROM {from} WHERE {left} UNION ALL SELECT {items} FROM {from} WHERE {right}",
            from = child.from
        );
        Relation {
            from: format!(
                "(SELECT {} FROM ({inner}) AS {alias}u) AS {alias}",
                names.join(", ")
            ),
            columns,
            unique: None,
            must_read: Self::mapped(&child.must_read, &map),
            has_union: true,
            has_join: false,
        }
    }

    /// One query: an outer `SELECT` over a relation nested up to `depth` deep.
    pub fn query(&mut self, depth: u32) -> String {
        let r = self.relation(depth);
        let numeric: Vec<Column> = r
            .columns
            .iter()
            .filter(|c| matches!(c.family, Family::Int | Family::Decimal | Family::Float))
            .cloned()
            .collect();
        // Aggregates of the columns the relation must have read, for the forms
        // that read no column of their own.
        let reads: String = r
            .must_read
            .iter()
            .flat_map(|c| [", max(", c.as_str(), ") AS m_", c.as_str()])
            .collect();
        let listed = |keep: &[Column]| {
            keep.iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self.rng.int(0, 7) {
            0 => format!("SELECT * FROM {}", r.from),
            1 => format!("SELECT count(*) AS n{reads} FROM {}", r.from),
            2 | 3 => {
                let keep = self.subset_reading(&r);
                format!("SELECT {} FROM {}", listed(&keep), r.from)
            }
            4 if r.must_read.is_empty() => format!("SELECT 1 AS one FROM {}", r.from),
            4 => format!(
                "SELECT 1 AS one, {} FROM {}",
                r.must_read.join(", "),
                r.from
            ),
            5 if !numeric.is_empty() => {
                let c = &numeric[self.index(numeric.len())].name;
                format!(
                    "SELECT count(*) AS n, sum({c}) AS s, min({c}) AS lo{reads} FROM {}",
                    r.from
                )
            }
            6 => {
                let predicate = self.predicate(&r.columns, 2);
                let keep = self.subset_reading(&r);
                format!("SELECT {} FROM {} WHERE {predicate}", listed(&keep), r.from)
            }
            _ => match Self::orderable(&r) {
                Some(unique) => {
                    let order: Vec<String> = unique
                        .iter()
                        .map(|k| format!("{k} ASC NULLS LAST"))
                        .collect();
                    format!(
                        "SELECT * FROM {} ORDER BY {} LIMIT {}",
                        r.from,
                        order.join(", "),
                        self.rng.int(1, 20)
                    )
                }
                None => format!("SELECT count(*) AS n{reads} FROM {}", r.from),
            },
        }
    }
}
