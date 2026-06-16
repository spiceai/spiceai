// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion_common::{Column, Result, ScalarValue, plan_err, stats::Precision};
use datafusion_expr::{Expr, JoinType, LogicalPlan, Operator};
use datafusion::datasource::DefaultTableSource;

use super::join_graph::Edge;

/// Fraction of preserved-side rows estimated to survive a semi/anti join
/// when column NDV statistics are unavailable. Mirrors DuckDB's
/// `CardinalityEstimator::DEFAULT_SEMI_ANTI_SELECTIVITY = 1/5`.
const DEFAULT_SEMI_ANTI_SELECTIVITY: f64 = 0.2;

/// Selectivity applied to a single-table filter predicate whose shape we
/// cannot size from statistics (LIKE, unknown UDFs, ...).
const DEFAULT_FILTER_SELECTIVITY: f64 = 0.1;

/// Selectivity applied to an open range comparison (`<`, `>`, `BETWEEN`, ...).
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.3;

/// Per literal (non-wildcard) character in a LIKE pattern. Postgres
/// `FIXED_CHAR_SEL` (`selfuncs.c`).
const LIKE_FIXED_CHAR_SEL: f64 = 0.20;

/// Per `_` (single-character wildcard) in a LIKE pattern. Postgres
/// `ANY_CHAR_SEL`.
const LIKE_ANY_CHAR_SEL: f64 = 0.9;

/// Per interior `%` (multi-character wildcard) in a LIKE pattern. Postgres
/// `FULL_WILDCARD_SEL`. Leading `%`/`_` are skipped, not multiplied.
const LIKE_FULL_WILDCARD_SEL: f64 = 5.0;

/// Floor so long literal patterns never estimate zero surviving rows.
const LIKE_MIN_SELECTIVITY: f64 = 0.0005;

pub trait JoinCostEstimator: std::fmt::Debug {
    /// Cardinality of `plan`.
    ///
    /// - `column = None`: number of output rows of `plan`.
    /// - `column = Some(c)`: number of distinct values of column `c`
    ///   in `plan`'s output (NDV).
    fn cardinality(&self, plan: &LogicalPlan, column: Option<&Column>) -> Option<f64> {
        estimate_cardinality(plan, column).ok()
    }

    /// Estimated selectivity of joining `left` with `right` via `edge`.
    ///
    /// Default: `1 / max(NDV(left.key), NDV(right.key))` for equi-joins
    /// (inner and semi/anti) when both NDVs are available; otherwise a
    /// per-join-type constant.
    fn selectivity(&self, edge: &Edge, left: &LogicalPlan, right: &LogicalPlan) -> f64 {
        let fallback = match edge.join_type {
            JoinType::Inner => 0.1,
            JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::RightSemi
            | JoinType::RightAnti => DEFAULT_SEMI_ANTI_SELECTIVITY,
            _ => 1.0,
        };
        let is_eq_join = matches!(
            edge.join_type,
            JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::RightSemi
                | JoinType::RightAnti
        );
        if !is_eq_join || edge.on.is_empty() {
            return fallback;
        }
        // Estimate from the first equi-pair only. Composing 1/max(NDV) across
        // all pairs (the PK-FK-correct product) was tried and is a no-op on
        // the chbench join orders, so it isn't worth the broader risk.
        let (a, b) = &edge.on[0];
        let ndv_a = key_side_ndv(self, a, left, right);
        let ndv_b = key_side_ndv(self, b, left, right);
        match edge.join_type {
            JoinType::Inner => match (ndv_a, ndv_b) {
                (Some(a), Some(b)) if a.max(b) > 0.0 => 1.0 / a.max(b),
                _ => fallback,
            },
            // Semi/anti containment estimator: surviving fraction of the
            // preserved side ≈ `min(NDV_preserved, NDV_filtering) / NDV_preserved`.
            // Edges normalized by `flatten_joins_recursive` always have
            // `on = (preserved_key, filtering_key)`, so the preserved
            // NDV is `ndv_a` for Left{Semi,Anti}. RightSemi/RightAnti
            // shouldn't appear in graph edges (they get normalized) but
            // are handled defensively.
            JoinType::LeftSemi | JoinType::LeftAnti => match (ndv_a, ndv_b) {
                (Some(a), Some(b)) if a > 0.0 => (a.min(b) / a).min(1.0),
                _ => fallback,
            },
            JoinType::RightSemi | JoinType::RightAnti => match (ndv_a, ndv_b) {
                (Some(a), Some(b)) if b > 0.0 => (a.min(b) / b).min(1.0),
                _ => fallback,
            },
            _ => fallback,
        }
    }

    fn cost(&self, selectivity: f64, cardinality: f64) -> f64 {
        selectivity * cardinality
    }
}

/// Default implementation of JoinCostEstimator
#[derive(Debug, Clone, Copy)]
pub struct DefaultCostEstimator;

impl JoinCostEstimator for DefaultCostEstimator {}

/// NDV (or an upper bound thereof) of a join-key expression, evaluated on
/// whichever input (`left`/`right`) owns the referenced columns.
///
/// A plain `Expr::Column` key returns its stored NDV. A computed key is
/// bounded by the product of its referenced columns' NDVs, capped by the
/// owning side's row count and — for a top-level `% k` — by `k`.
fn key_side_ndv<E: JoinCostEstimator + ?Sized>(
    estimator: &E,
    expr: &Expr,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Option<f64> {
    let cols = expr.column_refs();
    let rows_bound =
        if !cols.is_empty() && cols.iter().all(|c| left.schema().has_column(c)) {
            estimator.cardinality(left, None)
        } else if !cols.is_empty() && cols.iter().all(|c| right.schema().has_column(c)) {
            estimator.cardinality(right, None)
        } else {
            None
        };
    let lookup = |c: &Column| ndv_for(estimator, c, left, right);
    key_expr_ndv_bound(expr, &lookup, rows_bound)
}

/// Shared NDV-bound logic for a (possibly computed) key expression.
/// `col_ndv` resolves a referenced column's NDV; `rows_bound` is an optional
/// cap from the owning relation's row count.
fn key_expr_ndv_bound(
    expr: &Expr,
    col_ndv: &dyn Fn(&Column) -> Option<f64>,
    rows_bound: Option<f64>,
) -> Option<f64> {
    if let Expr::Column(c) = expr {
        return col_ndv(c);
    }
    let modulo_cap = modulo_literal_cap(expr);
    let cols = expr.column_refs();
    if cols.is_empty() {
        // No column refs (pure literal/parameter): only a modulo literal can
        // bound the distinct count.
        return modulo_cap;
    }
    let mut product = 1.0_f64;
    let mut any = false;
    for c in &cols {
        if let Some(n) = col_ndv(c) {
            product *= n.max(1.0);
            any = true;
        }
    }
    if !any {
        return modulo_cap;
    }
    let mut bound = product;
    if let Some(r) = rows_bound {
        bound = bound.min(r);
    }
    if let Some(cap) = modulo_cap {
        bound = bound.min(cap);
    }
    Some(bound.max(1.0))
}

/// If `expr` is (modulo a wrapping cast/alias) a modulo by a positive integer
/// literal `k`, returns `k` — an exact upper bound on the result's NDV. Matches
/// both spellings: the `X % k` operator and the `mod(X, k)` scalar function
/// (the form chbench/SQL `mod(...)` emits — it survives planning as a scalar
/// function, not the `%` operator, so both must be recognized).
fn modulo_literal_cap(expr: &Expr) -> Option<f64> {
    match unwrap_cast_alias(expr) {
        Expr::BinaryExpr(be) if be.op == Operator::Modulo => {
            literal_as_f64(&be.right).filter(|v| *v >= 1.0)
        }
        Expr::ScalarFunction(sf)
            if sf.func.name().eq_ignore_ascii_case("mod") && sf.args.len() == 2 =>
        {
            literal_as_f64(&sf.args[1]).filter(|v| *v >= 1.0)
        }
        _ => None,
    }
}

/// Peels `Cast`/`TryCast`/`Alias` wrappers to expose the inner expression.
fn unwrap_cast_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(c) => unwrap_cast_alias(&c.expr),
        Expr::TryCast(c) => unwrap_cast_alias(&c.expr),
        Expr::Alias(a) => unwrap_cast_alias(&a.expr),
        other => other,
    }
}

/// Extracts a non-negative integer literal as `f64`, peeling cast/alias wrappers.
fn literal_as_f64(expr: &Expr) -> Option<f64> {
    let Expr::Literal(sv, _) = unwrap_cast_alias(expr) else {
        return None;
    };
    match sv {
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt16(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        _ => None,
    }
}

/// Extracts a UTF-8 string literal (a LIKE pattern), peeling cast/alias wrappers.
fn literal_pattern(expr: &Expr) -> Option<String> {
    let Expr::Literal(sv, _) = unwrap_cast_alias(expr) else {
        return None;
    };
    match sv {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Postgres-style LIKE selectivity (`like_selectivity` in `selfuncs.c`).
///
/// Leading `%`/`_` wildcards are skipped (they are already accounted for by the
/// base selectivity of 1.0); each remaining pattern character then contributes a
/// multiplier: a literal char → `LIKE_FIXED_CHAR_SEL`, `_` → `LIKE_ANY_CHAR_SEL`,
/// an interior `%` → `LIKE_FULL_WILDCARD_SEL`. The escape char quotes the next
/// character into a literal. The product is clamped to `[floor, 1.0]`.
fn like_selectivity(pattern: &str, escape: Option<char>) -> f64 {
    let chars: Vec<char> = pattern.chars().collect();
    let mut pos = 0;
    // Skip leading wildcards: a leading `%`/`_` is already factored into the
    // base selectivity of 1.0 and does not narrow the match.
    while pos < chars.len() && (chars[pos] == '%' || chars[pos] == '_') {
        pos += 1;
    }
    let mut sel = 1.0_f64;
    while pos < chars.len() {
        let c = chars[pos];
        if Some(c) == escape {
            // Escape quotes the following character as a literal.
            pos += 1;
            if pos >= chars.len() {
                break;
            }
            sel *= LIKE_FIXED_CHAR_SEL;
        } else if c == '%' {
            sel *= LIKE_FULL_WILDCARD_SEL;
        } else if c == '_' {
            sel *= LIKE_ANY_CHAR_SEL;
        } else {
            sel *= LIKE_FIXED_CHAR_SEL;
        }
        pos += 1;
    }
    sel.clamp(LIKE_MIN_SELECTIVITY, 1.0)
}


/// `(0, 1]`.
fn predicate_selectivity(pred: &Expr, input: &LogicalPlan) -> f64 {
    match pred {
        Expr::Alias(a) => predicate_selectivity(&a.expr, input),
        Expr::Not(inner) => 1.0 - predicate_selectivity(inner, input),
        Expr::Like(like) => {
            let base = match literal_pattern(&like.pattern) {
                Some(p) => like_selectivity(&p, like.escape_char),
                None => DEFAULT_FILTER_SELECTIVITY,
            };
            if like.negated { 1.0 - base } else { base }
        }
        Expr::Between(b) => {
            if b.negated {
                1.0 - DEFAULT_RANGE_SELECTIVITY
            } else {
                DEFAULT_RANGE_SELECTIVITY
            }
        }
        Expr::InList(inlist) => {
            let base = match column_ndv_in(&inlist.expr, input) {
                Some(ndv) if ndv > 0.0 => {
                    (inlist.list.len() as f64 / ndv).clamp(0.0, 1.0)
                }
                _ => DEFAULT_FILTER_SELECTIVITY,
            };
            if inlist.negated { 1.0 - base } else { base }
        }
        Expr::BinaryExpr(be) => match be.op {
            Operator::And => {
                predicate_selectivity(&be.left, input)
                    * predicate_selectivity(&be.right, input)
            }
            Operator::Or => {
                let l = predicate_selectivity(&be.left, input);
                let r = predicate_selectivity(&be.right, input);
                (l + r - l * r).clamp(0.0, 1.0)
            }
            Operator::Eq => equality_selectivity(&be.left, &be.right, input),
            Operator::NotEq => {
                1.0 - equality_selectivity(&be.left, &be.right, input)
            }
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                DEFAULT_RANGE_SELECTIVITY
            }
            _ => DEFAULT_FILTER_SELECTIVITY,
        },
        _ => DEFAULT_FILTER_SELECTIVITY,
    }
}

/// `col = literal` selectivity ≈ `1 / NDV(col)`. Either operand may be the
/// column; any other shape falls back to the default constant.
fn equality_selectivity(left: &Expr, right: &Expr, input: &LogicalPlan) -> f64 {
    let col = match (left, right) {
        (Expr::Column(c), r) if is_literal(r) => Some(c),
        (l, Expr::Column(c)) if is_literal(l) => Some(c),
        _ => None,
    };
    match col.and_then(|c| estimate_cardinality(input, Some(c)).ok()) {
        Some(ndv) if ndv > 0.0 => (1.0 / ndv).clamp(0.0, 1.0),
        _ => DEFAULT_FILTER_SELECTIVITY,
    }
}

/// NDV of `expr` on `input` when `expr` is a plain column reference.
fn column_ndv_in(expr: &Expr, input: &LogicalPlan) -> Option<f64> {
    match expr {
        Expr::Column(c) => estimate_cardinality(input, Some(c)).ok(),
        _ => None,
    }
}

fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_, _))
}

/// Look up NDV of `column` on whichever side (left or right) owns it.
fn ndv_for<E: JoinCostEstimator + ?Sized>(
    estimator: &E,
    column: &Column,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Option<f64> {
    if left.schema().has_column(column) {
        estimator.cardinality(left, Some(column))
    } else if right.schema().has_column(column) {
        estimator.cardinality(right, Some(column))
    } else {
        None
    }
}

pub(super) fn estimate_cardinality(plan: &LogicalPlan, column: Option<&Column>) -> Result<f64> {
    match plan {
        LogicalPlan::Filter(filter) => match column {
            None => {
                let input = estimate_cardinality(&filter.input, None)?;
                let sel = predicate_selectivity(&filter.predicate, &filter.input);
                Ok((sel * input).max(1.0))
            }
            Some(c) => {
                // NDV is bounded above by the input's NDV and by the
                // surviving row count.
                let ndv_in = estimate_cardinality(&filter.input, Some(c))?;
                let rows_out = estimate_cardinality(plan, None).unwrap_or(ndv_in);
                Ok(ndv_in.min(rows_out))
            }
        },
        LogicalPlan::Aggregate(agg) => match column {
            None => {
                // Ungrouped aggregate → exactly 1 row.
                if agg.group_expr.is_empty() {
                    return Ok(1.0);
                }
                let input = estimate_cardinality(&agg.input, None)?;
                // Per-group-key NDV from the child plan, where available.
                // Mirrors duckdb's `ExtractAggregationStats`
                // (relation_statistics_helper.cpp:380-415): start with the
                // product of per-key NDVs, apply a correlation correction,
                // then use the Occupancy-Problem formula to estimate the
                // number of group-key tuples actually occupied given
                // `input` rows.
                let ndvs: Vec<f64> = agg
                    .group_expr
                    .iter()
                    .filter_map(|e| match e {
                        Expr::Column(c) => Some(c),
                        _ => None,
                    })
                    .filter_map(|c| estimate_cardinality(&agg.input, Some(c)).ok())
                    .map(|n| if n <= 0.0 { 1.0 } else { n })
                    .collect();
                if ndvs.is_empty() || ndvs.len() < agg.group_expr.len() {
                    // No (or partial) per-key NDV. Half the input is a
                    // less-pessimistic default than `0.1 * input`, matching
                    // duckdb's fallback at relation_statistics_helper.cpp:394.
                    return Ok((input / 2.0).max(1.0));
                }
                let product: f64 = ndvs.iter().product();
                let correction = 0.95_f64.powi((ndvs.len() as i32) - 1);
                let product = product * correction;
                let mult = 1.0 - (-input / product).exp();
                let new_card = if mult == 0.0 { input } else { product * mult };
                Ok(new_card.min(input).max(1.0))
            }
            Some(c) => {
                // Group-by keys are unique in the aggregate's output, so
                // NDV(group_key) equals the post-aggregate row count.
                // Match by column name only — a SubqueryAlias wrapping the
                // aggregate rewrites the relation prefix, so a strict
                // `relation == relation` comparison would miss legitimate
                // group keys.
                let is_group_key = agg.group_expr.iter().any(|e| match e {
                    Expr::Column(g) => g.name == c.name,
                    _ => false,
                });
                if is_group_key {
                    estimate_cardinality(plan, None)
                } else {
                    // For non-group columns, the post-aggregate NDV is
                    // bounded by the row count (most one distinct value per
                    // output row). Return that as a loose upper bound
                    // instead of erroring, so callers (e.g.
                    // `selectivity()`) can still compute a fallback.
                    estimate_cardinality(plan, None)
                }
            }
        },
        LogicalPlan::TableScan(scan) => {
            // DataFusion's logical layer has no statistics hook on
            // `TableSource`, so reach the underlying `TableProvider`
            // (which spiceai providers — Cayenne, accelerated, dataset —
            // implement `statistics()` on) via the standard
            // `DefaultTableSource` wrapper.
            let stats = scan
                .source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
                .and_then(|src| src.table_provider.statistics())
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "TableSource for `{}` does not expose statistics",
                        scan.table_name
                    ))
                })?;
            match column {
                None => match stats.num_rows {
                    Precision::Exact(n) | Precision::Inexact(n) => Ok(n as f64),
                    Precision::Absent => plan_err!(
                        "TableSource for `{}` does not provide a row count",
                        scan.table_name
                    ),
                },
                Some(c) => {
                    // `column_statistics` is indexed by the source schema
                    // (pre-projection), so resolve the column there.
                    let idx = scan.source.schema().index_of(&c.name).map_err(|_| {
                        datafusion_common::DataFusionError::Plan(format!(
                            "Column `{}` not found in source schema of `{}`",
                            c.name, scan.table_name
                        ))
                    })?;
                    let col_stats =
                        stats.column_statistics.get(idx).ok_or_else(|| {
                            datafusion_common::DataFusionError::Plan(format!(
                                "Column statistics missing for index {idx} \
                                 on `{}`",
                                scan.table_name
                            ))
                        })?;
                    match col_stats.distinct_count {
                        Precision::Exact(n) | Precision::Inexact(n) => Ok(n as f64),
                        Precision::Absent => plan_err!(
                            "Column `{}` on `{}` has no distinct-count statistic",
                            c.name,
                            scan.table_name
                        ),
                    }
                }
            }
        }
        // Semi/anti joins do not grow rows: the output cardinality is
        // bounded by the preserved side. We size them via the
        // `DEFAULT_SEMI_ANTI_SELECTIVITY` heuristic. NDV queries on the
        // output route to whichever side is preserved.
        LogicalPlan::Join(j)
            if matches!(
                j.join_type,
                JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightSemi
                    | JoinType::RightAnti
            ) =>
        {
            let preserved = match j.join_type {
                JoinType::LeftSemi | JoinType::LeftAnti => &j.left,
                _ => &j.right,
            };
            match column {
                None => {
                    let rows = estimate_cardinality(preserved, None)?;
                    Ok(rows * DEFAULT_SEMI_ANTI_SELECTIVITY)
                }
                Some(c) => estimate_cardinality(preserved, Some(c)),
            }
        }
        // Inner joins (and the cross-product, encoded as Inner with empty
        // `on`) appear here when an upstream caller asks about a join
        // subtree that the flattener absorbed as an opaque graph node
        // (e.g. when a projection or other wrapper sits between joins).
        // Estimate via the same NDV-of-the-largest-side formula
        // `selectivity()` uses for inner equi-joins, falling back to 0.1
        // when NDV is unavailable.
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
            let left_card = estimate_cardinality(&j.left, None)?;
            let right_card = estimate_cardinality(&j.right, None)?;
            let cross = left_card * right_card;
            let sel = if let Some((a, b)) = j.on.first() {
                let col_ndv = |c: &Column| {
                    estimate_cardinality(&j.left, Some(c))
                        .ok()
                        .or_else(|| estimate_cardinality(&j.right, Some(c)).ok())
                };
                let rows_bound = |e: &Expr| {
                    let cols = e.column_refs();
                    if !cols.is_empty()
                        && cols.iter().all(|c| j.left.schema().has_column(c))
                    {
                        estimate_cardinality(&j.left, None).ok()
                    } else if !cols.is_empty()
                        && cols.iter().all(|c| j.right.schema().has_column(c))
                    {
                        estimate_cardinality(&j.right, None).ok()
                    } else {
                        None
                    }
                };
                let na = key_expr_ndv_bound(a, &col_ndv, rows_bound(a));
                let nb = key_expr_ndv_bound(b, &col_ndv, rows_bound(b));
                let ndv_max = match (na, nb) {
                    (Some(x), Some(y)) if x.max(y) > 0.0 => Some(x.max(y)),
                    (Some(x), None) | (None, Some(x)) if x > 0.0 => Some(x),
                    _ => None,
                };
                ndv_max.map(|n| 1.0 / n).unwrap_or(0.1)
            } else {
                1.0
            };
            match column {
                None => Ok((sel * cross).max(1.0)),
                Some(c) => {
                    // NDV of a column on the join output is bounded by the
                    // child-side NDV (joins don't create new distinct values
                    // for already-existing columns).
                    estimate_cardinality(&j.left, Some(c))
                        .or_else(|_| estimate_cardinality(&j.right, Some(c)))
                }
            }
        }
        x => {
            let inputs = x.inputs();
            if inputs.len() == 1 {
                estimate_cardinality(inputs[0], column)
            } else {
                plan_err!("Cannot estimate cardinality for plan with multiple inputs")
            }
        }
    }
}

