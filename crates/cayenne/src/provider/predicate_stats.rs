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

//! Per-table filter-column hit histogram for default-on adaptive cold layout (F4).
//!
//! Scans record which columns appear in pushdown filters. When operators leave
//! `sort_columns` / `cluster_by` empty, compaction consults
//! [`FilterColumnObservations::top_columns`] and sorts the rewrite by the hottest
//! columns so zone maps prune selective queries without any spicepod setup.
//!
//! Scans also record which columns a filter pins to a few values, and which it
//! only bounds by a literal ([`FilterColumnObservations::pinned_hits`]). Key-range
//! routing ranks columns by those: a file holding one range of a column is skipped
//! only by a filter that names few of its ranges, so a column that queries
//! reference only in a long `IN` list, or in a join, would route files no lookup
//! can skip. A pinned value outranks a bound — a lookup that fixes one column and
//! bounds another by a witness such as `count >= 1` is served by routing on the
//! fixed one — so bounds decide only between columns no scan pins to values.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use parking_lot::Mutex;

/// Maximum distinct filter columns retained in the histogram.
const MAX_TRACKED_COLUMNS: usize = 32;

/// Default number of hot columns to feed into compaction sort / clustering.
pub(crate) const DEFAULT_AUTO_CLUSTER_TOP_K: usize = 2;

/// The most distinct values a filter may pin a column to and still count as
/// naming few of its key ranges: an equality, or a short `IN` list.
const MAX_PINNED_VALUES: usize = 4;

/// One column's hit counts.
#[derive(Debug, Default, Clone, Copy)]
struct ColumnHits {
    /// Scans whose filters reference the column at all.
    any: u64,
    /// Scans whose filters pin or bound the column ([`PinnedHits`]).
    pinned: PinnedHits,
}

/// How many scans' filters pinned a column to at most [`MAX_PINNED_VALUES`]
/// literal values, and how many only bounded it by a literal.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PinnedHits {
    pub(crate) values: u64,
    pub(crate) bounds: u64,
}

impl PinnedHits {
    /// Whether these hits lead `other`'s by more than twice: on pinned values
    /// when either column has any, else on bounds.
    pub(crate) fn lead_more_than_twice(&self, other: &Self) -> bool {
        if self.values > 0 || other.values > 0 {
            self.values > other.values.saturating_mul(2)
        } else {
            self.bounds > other.bounds.saturating_mul(2)
        }
    }
}

/// Bounded per-table filter-column hit counts shared across provider clones.
#[derive(Debug, Default)]
pub(crate) struct FilterColumnObservations {
    state: Mutex<HashMap<String, ColumnHits>>,
}

impl FilterColumnObservations {
    /// Create an empty observation map.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Record columns referenced by the given scan filters (one hit per
    /// distinct column name per call, not per occurrence in a single expression).
    pub(crate) fn record_filters(&self, filters: &[Expr]) {
        if filters.is_empty() {
            return;
        }
        let mut names = Vec::new();
        for filter in filters {
            collect_column_names(filter, &mut names);
        }
        if names.is_empty() {
            return;
        }
        let pinned = pinned_columns(filters);
        let mut state = self.state.lock();
        for name in names {
            let pin = pinned
                .iter()
                .find(|(column, _)| *column == name)
                .map(|(_, kind)| *kind);
            let entry = state.entry(name).or_default();
            entry.any = entry.any.saturating_add(1);
            match pin {
                Some(PinKind::Values) => {
                    entry.pinned.values = entry.pinned.values.saturating_add(1);
                }
                Some(PinKind::Bound) => {
                    entry.pinned.bounds = entry.pinned.bounds.saturating_add(1);
                }
                None => {}
            }
        }
        // Cap cardinality: drop lowest-hit keys when over the limit so memory
        // stays bounded under wide schemas / ad-hoc column churn.
        while state.len() > MAX_TRACKED_COLUMNS {
            // Deterministic victim: the coldest column, breaking ties by name so
            // eviction never depends on the randomized `HashMap` iteration order
            // (non-determinism there would leak into the observed set and thus
            // auto-layout under column churn). Ties retain the lexicographically
            // smaller name, matching `top_columns`' tie-break.
            let victim = state
                .iter()
                .min_by(|a, b| a.1.any.cmp(&b.1.any).then_with(|| b.0.cmp(a.0)))
                .map(|(name, _)| name.clone());
            if let Some(name) = victim {
                state.remove(&name);
            } else {
                break;
            }
        }
    }

    /// Return up to `top_k` column names with the highest hit counts that exist
    /// in `schema`, highest first. Empty when nothing useful has been observed.
    #[must_use]
    pub(crate) fn top_columns(&self, top_k: usize, schema: &Schema) -> Vec<String> {
        self.ranked(top_k, schema, |hits| (hits.any, 0))
    }

    /// Return up to `top_k` column names in `schema` that filters have pinned or
    /// bounded ([`Self::pinned_hits`]): most pinned values first, then most
    /// bounds. Empty when no filter has done either yet.
    #[must_use]
    pub(crate) fn top_pinned_columns(&self, top_k: usize, schema: &Schema) -> Vec<String> {
        self.ranked(top_k, schema, |hits| {
            (hits.pinned.values, hits.pinned.bounds)
        })
    }

    fn ranked(
        &self,
        top_k: usize,
        schema: &Schema,
        count: impl Fn(&ColumnHits) -> (u64, u64),
    ) -> Vec<String> {
        if top_k == 0 {
            return Vec::new();
        }
        let state = self.state.lock();
        let mut ranked: Vec<(&str, (u64, u64))> = state
            .iter()
            .filter(|(name, _)| schema.index_of(name.as_str()).is_ok())
            .map(|(name, hits)| (name.as_str(), count(hits)))
            .filter(|(_, hits)| *hits != (0, 0))
            .collect();
        // Stable tie-break by name so compaction layout is deterministic under
        // equal hit counts (tests and zone-map quality both care).
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        ranked
            .into_iter()
            .take(top_k)
            .map(|(name, _)| name.to_string())
            .collect()
    }

    /// Hit count for a column (0 if never observed).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn hits(&self, column: &str) -> u64 {
        self.state.lock().get(column).map_or(0, |hits| hits.any)
    }

    /// How many scans' filters pinned a column to at most [`MAX_PINNED_VALUES`]
    /// values, and how many only bounded it by a literal (zeros if never).
    #[must_use]
    pub(crate) fn pinned_hits(&self, column: &str) -> PinnedHits {
        self.state
            .lock()
            .get(column)
            .map_or_else(PinnedHits::default, |hits| hits.pinned)
    }
}

/// How a filter constrains one column.
#[derive(Debug, Clone)]
enum Pin {
    /// To one of these literal values. Holds at most `MAX_PINNED_VALUES + 1`:
    /// past that the count no longer matters.
    Values(Vec<ScalarValue>),
    /// To a range bounded by a literal on at least one side.
    Range,
}

/// Whether a filter pins a column to a few values or only bounds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PinKind {
    Values,
    Bound,
}

/// The columns a conjunction of `filters` pins to at most
/// [`MAX_PINNED_VALUES`] values, or bounds by a literal.
fn pinned_columns(filters: &[Expr]) -> Vec<(String, PinKind)> {
    let mut pins = HashMap::new();
    for filter in filters {
        pins = and_pins(pins, pins_of(filter));
    }
    pins.into_iter()
        .filter_map(|(column, pin)| match pin {
            Pin::Values(values) if values.len() <= MAX_PINNED_VALUES => {
                Some((column, PinKind::Values))
            }
            Pin::Values(_) => None,
            Pin::Range => Some((column, PinKind::Bound)),
        })
        .collect()
}

/// The columns `expr` pins, and how. A disjunction pins a column only when
/// every branch does — `(w = 7 AND i = 1) OR (w = 7 AND i = 2)` pins `w` to one
/// value and `i` to two.
fn pins_of(expr: &Expr) -> HashMap<String, Pin> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => and_pins(pins_of(left), pins_of(right)),
            Operator::Or => or_pins(pins_of(left), pins_of(right)),
            Operator::Eq => column_and_literal(left, right)
                .map(|(column, value)| HashMap::from([(column, Pin::Values(vec![value]))]))
                .unwrap_or_default(),
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                column_and_literal(left, right)
                    .map(|(column, _)| HashMap::from([(column, Pin::Range)]))
                    .unwrap_or_default()
            }
            _ => HashMap::new(),
        },
        Expr::InList(in_list) if !in_list.negated => {
            let Some(column) = column_name(&in_list.expr) else {
                return HashMap::new();
            };
            let mut values = Vec::new();
            for item in &in_list.list {
                let Some(value) = literal(item) else {
                    return HashMap::new();
                };
                push_distinct(&mut values, value);
            }
            HashMap::from([(column, Pin::Values(values))])
        }
        Expr::Between(between)
            if !between.negated
                && literal(&between.low).is_some()
                && literal(&between.high).is_some() =>
        {
            column_name(&between.expr)
                .map(|column| HashMap::from([(column, Pin::Range)]))
                .unwrap_or_default()
        }
        _ => HashMap::new(),
    }
}

/// Both sides hold: each column keeps the narrower of its two pins.
fn and_pins(mut left: HashMap<String, Pin>, right: HashMap<String, Pin>) -> HashMap<String, Pin> {
    for (column, pin) in right {
        let narrower = match left.remove(&column) {
            Some(Pin::Values(a)) => match pin {
                Pin::Values(b) if b.len() < a.len() => Pin::Values(b),
                _ => Pin::Values(a),
            },
            None | Some(Pin::Range) => pin,
        };
        left.insert(column, narrower);
    }
    left
}

/// Either side holds: only columns both sides pin stay pinned, to the union.
fn or_pins(left: HashMap<String, Pin>, mut right: HashMap<String, Pin>) -> HashMap<String, Pin> {
    let mut union = HashMap::new();
    for (column, pin) in left {
        let Some(other) = right.remove(&column) else {
            continue;
        };
        let joined = match (pin, other) {
            (Pin::Values(mut values), Pin::Values(more)) => {
                for value in more {
                    push_distinct(&mut values, value);
                }
                Pin::Values(values)
            }
            _ => Pin::Range,
        };
        union.insert(column, joined);
    }
    union
}

/// Adds `value` unless present, keeping at most `MAX_PINNED_VALUES + 1` values.
fn push_distinct(values: &mut Vec<ScalarValue>, value: ScalarValue) {
    if values.len() <= MAX_PINNED_VALUES && !values.contains(&value) {
        values.push(value);
    }
}

/// `(column, literal)` when one side of a comparison is a column and the other
/// a literal, either way round.
fn column_and_literal(left: &Expr, right: &Expr) -> Option<(String, ScalarValue)> {
    match (column_name(left), literal(right)) {
        (Some(column), Some(value)) => Some((column, value)),
        _ => Some((column_name(right)?, literal(left)?)),
    }
}

/// The column `expr` names, seeing through casts.
fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Cast(cast) => column_name(&cast.expr),
        Expr::TryCast(cast) => column_name(&cast.expr),
        _ => None,
    }
}

/// The literal `expr` is, seeing through casts.
fn literal(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(cast) => literal(&cast.expr),
        Expr::TryCast(cast) => literal(&cast.expr),
        _ => None,
    }
}

/// Shared handle stored on [`crate::provider::table::CayenneTableProvider`].
pub(crate) type SharedFilterColumnObservations = Arc<FilterColumnObservations>;

fn collect_column_names(expr: &Expr, out: &mut Vec<String>) {
    let _ = expr.apply(|node| {
        if let Expr::Column(column) = node {
            let name = column.name.as_str();
            if !out.iter().any(|existing| existing == name) {
                out.push(name.to_string());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use datafusion_expr::{col, lit};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ])
    }

    #[test]
    fn record_filters_counts_distinct_columns_per_call() {
        let observations = FilterColumnObservations::new();
        let filters = vec![col("region").eq(lit("west")), col("amount").gt(lit(10_i64))];
        observations.record_filters(&filters);
        observations.record_filters(&filters);
        assert_eq!(observations.hits("region"), 2);
        assert_eq!(observations.hits("amount"), 2);
        assert_eq!(observations.hits("ts"), 0);
    }

    /// A column counts as pinned where a filter fixes it to at most
    /// `MAX_PINNED_VALUES` values or bounds it by a literal, through `AND` and
    /// through an `OR` whose every branch pins it.
    #[test]
    fn pinned_columns_follow_equalities_short_lists_and_bounds() {
        let observations = FilterColumnObservations::new();
        let lookup = (0..15_i64)
            .map(|k| col("pk").eq(lit(7_i64)).and(col("amount").eq(lit(k))))
            .reduce(Expr::or)
            .expect("fifteen branches");
        observations.record_filters(&[lookup]);
        observations.record_filters(&[col("region").in_list(vec![lit("a"), lit("b")], false)]);
        observations.record_filters(&[
            col("ts").gt(lit(5_i64)),
            col("amount").in_list((0..10_i64).map(lit).collect(), false),
        ]);
        observations.record_filters(&[col("pk").eq(lit(1_i64)).or(col("region").eq(lit("x")))]);
        let values = |values| PinnedHits { values, bounds: 0 };
        assert_eq!(
            observations.pinned_hits("pk"),
            values(1),
            "every branch pins pk to the same value; one side of an OR pins nothing"
        );
        assert_eq!(
            observations.pinned_hits("amount"),
            PinnedHits::default(),
            "fifteen values, and then ten, are too many"
        );
        assert_eq!(
            observations.pinned_hits("region"),
            values(1),
            "a two-value list pins"
        );
        assert_eq!(
            observations.pinned_hits("ts"),
            PinnedHits {
                values: 0,
                bounds: 1
            },
            "a literal bound bounds the column without pinning it"
        );
        assert_eq!(
            (
                observations.hits("pk"),
                observations.hits("amount"),
                observations.hits("region"),
                observations.hits("ts")
            ),
            (2, 2, 2, 1),
            "every reference still counts as a hit"
        );
    }

    /// A pinned value outranks a bound: a receipt lookup that fixes `w`, lists
    /// a few values of `i` and bounds a witness `cnt >= 1` in every branch ranks
    /// `w` first, although `cnt` is bounded as often and sorts before it; and a
    /// bound decides only between columns no scan pins.
    #[test]
    fn a_pinned_value_outranks_a_bound() {
        let observations = FilterColumnObservations::new();
        let schema = Schema::new(vec![
            Field::new("cnt", DataType::Int64, false),
            Field::new("i", DataType::Int64, false),
            Field::new("w", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ]);
        let lookup = [1_i64, 6_152, 12_303, 18_454, 24_605, 30_756]
            .into_iter()
            .map(|item| {
                col("w")
                    .eq(lit(4_i64))
                    .and(col("i").eq(lit(item)))
                    .and(col("cnt").gt_eq(lit(1_i64)))
            })
            .reduce(Expr::or)
            .expect("six rows");
        for _ in 0..3 {
            observations.record_filters(std::slice::from_ref(&lookup));
        }
        assert_eq!(
            observations.top_pinned_columns(2, &schema),
            vec!["w".to_string(), "cnt".to_string()],
            "the pinned column ranks before the bounded one"
        );
        let (w, cnt) = (
            observations.pinned_hits("w"),
            observations.pinned_hits("cnt"),
        );
        assert!(w.lead_more_than_twice(&observations.pinned_hits("i")));
        assert!(
            !cnt.lead_more_than_twice(&w),
            "a bound never leads a pinned column"
        );
        for _ in 0..10 {
            observations.record_filters(&[col("ts").gt(lit(5_i64))]);
        }
        let none = PinnedHits::default();
        assert!(
            observations.pinned_hits("ts").lead_more_than_twice(&none),
            "where neither column is pinned, bounds decide"
        );
    }

    #[test]
    fn top_columns_ranks_by_hits_and_schema() {
        let observations = FilterColumnObservations::new();
        let schema = test_schema();
        for _ in 0..5 {
            observations.record_filters(&[col("amount").gt(lit(0_i64))]);
        }
        for _ in 0..2 {
            observations.record_filters(&[col("region").eq(lit("east"))]);
        }
        // Unknown column must not surface even if "hot".
        for _ in 0..20 {
            observations.record_filters(&[col("not_in_schema").eq(lit(1_i64))]);
        }
        let top = observations.top_columns(2, &schema);
        assert_eq!(top, vec!["amount".to_string(), "region".to_string()]);
        let top1 = observations.top_columns(1, &schema);
        assert_eq!(top1, vec!["amount".to_string()]);
    }

    #[test]
    fn top_columns_empty_without_observations() {
        let observations = FilterColumnObservations::new();
        let schema = test_schema();
        assert!(observations.top_columns(2, &schema).is_empty());
    }

    #[test]
    fn nested_and_or_still_collects_leaf_columns() {
        let observations = FilterColumnObservations::new();
        let schema = test_schema();
        let filter = col("region")
            .eq(lit("west"))
            .and(col("amount").gt(lit(5_i64)).or(col("ts").lt(lit(100_i64))));
        observations.record_filters(&[filter]);
        let mut top = observations.top_columns(3, &schema);
        top.sort();
        assert_eq!(
            top,
            vec!["amount".to_string(), "region".to_string(), "ts".to_string()]
        );
    }
}
