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

//! Translation of DataFusion [`Expr`] filters into tantivy queries, and the matching
//! [`TableProviderFilterPushDown`] classification.
//!
//! Correctness invariant: [`classify_filter`] and [`translate_filter`] are two views over the
//! same [`translate`] pass, so a filter reported as `Exact`/`Inexact` is exactly the one the
//! executor can build a tantivy query for, and one reported `Unsupported` never is. A filter
//! reported `Exact` produces a tantivy query whose match set equals the SQL predicate; a filter
//! reported `Inexact` produces a query whose match set is a *superset* of the SQL predicate
//! (DataFusion re-checks it above the scan). Neither ever produces a subset — that would drop
//! rows and return wrong results.

use std::ops::Bound;

use arrow::compute::CastOptions;
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    Between, BinaryExpr, Expr, Like, Operator, TableProviderFilterPushDown,
};
use tantivy::Term;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery, TermQuery};
use tantivy::schema::{Field, FieldType, IndexRecordOption, Schema};

use super::index::is_tokenized;
use super::util::array_to_terms;

/// `safe: false` makes an out-of-range or wrong-signedness literal cast *error* rather than
/// silently saturate/null. A literal that does not fit the field's numeric type must not be
/// pushed (it would mis-encode the term); the error is caught and the filter falls back to
/// not-pushed.
const STRICT_CAST: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: arrow::util::display::FormatOptions::new(),
};

/// The tantivy value kind of an index column that a filter can be pushed against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldKind {
    I64,
    U64,
    F64,
    Bool,
    /// Untokenized `STRING` text (a primary key or explicit store column) — matchable by a
    /// single [`TermQuery`] on the raw value.
    StrExact,
}

impl FieldKind {
    /// The Arrow type a literal must be coerced to before encoding a term for this field.
    fn arrow_type(self) -> DataType {
        match self {
            FieldKind::I64 => DataType::Int64,
            FieldKind::U64 => DataType::UInt64,
            FieldKind::F64 => DataType::Float64,
            FieldKind::Bool => DataType::Boolean,
            FieldKind::StrExact => DataType::Utf8,
        }
    }

    /// Whether a literal of `dt` may be compared against this field. Guards against surprising
    /// cross-type coercions (e.g. treating an integer literal as text): only same-family
    /// literals are accepted, then range-checked by the strict cast in [`literal_to_term`].
    fn accepts(self, dt: &DataType) -> bool {
        match self {
            FieldKind::I64 | FieldKind::U64 => dt.is_integer(),
            // A float field may be compared against an integer or a float literal.
            FieldKind::F64 => dt.is_integer() || dt.is_floating(),
            FieldKind::Bool => matches!(dt, DataType::Boolean),
            FieldKind::StrExact => {
                matches!(dt, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
            }
        }
    }

    fn is_numeric(self) -> bool {
        matches!(self, FieldKind::I64 | FieldKind::U64 | FieldKind::F64)
    }
}

/// The outcome of translating a single filter (sub)expression.
enum Translated {
    /// The tantivy query's match set equals the SQL predicate.
    Exact(Box<dyn Query>),
    /// The tantivy query's match set is a superset of the SQL predicate (re-checked above the
    /// scan by DataFusion).
    Inexact(Box<dyn Query>),
    /// Cannot be pushed at all.
    Unsupported,
}

/// Classify each `filter` for [`datafusion::catalog::TableProvider::supports_filters_pushdown`].
pub(super) fn classify_filter(schema: &Schema, filter: &Expr) -> TableProviderFilterPushDown {
    match translate(schema, filter) {
        Translated::Exact(_) => TableProviderFilterPushDown::Exact,
        Translated::Inexact(_) => TableProviderFilterPushDown::Inexact,
        Translated::Unsupported => TableProviderFilterPushDown::Unsupported,
    }
}

/// Build the tantivy query for a filter DataFusion has pushed down, or [`None`] when the filter
/// cannot be translated. Because this shares [`translate`] with [`classify_filter`], `None` is
/// only ever returned for a filter that would have been classified `Unsupported` — such a filter
/// is never in the pushed set, so the executor treats `None` as an error rather than silently
/// dropping it.
pub(super) fn translate_filter(schema: &Schema, filter: &Expr) -> Option<Box<dyn Query>> {
    match translate(schema, filter) {
        Translated::Exact(q) | Translated::Inexact(q) => Some(q),
        Translated::Unsupported => None,
    }
}

fn translate(schema: &Schema, expr: &Expr) -> Translated {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => combine(schema, left, right, Occur::Must),
            Operator::Or => combine(schema, left, right, Occur::Should),
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => comparison(schema, left, *op, right),
            _ => Translated::Unsupported,
        },
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => between(schema, expr, *negated, low, high),
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => in_list(schema, expr, list, *negated),
        Expr::Like(like) => like_prefix(schema, like),
        // A negation over a nullable column includes the column's NULL rows (SQL excludes them),
        // so the pushed query is a superset — always `Inexact`, never `Exact`.
        Expr::Not(inner) => match translate(schema, inner) {
            Translated::Exact(q) | Translated::Inexact(q) => Translated::Inexact(negate(q)),
            Translated::Unsupported => Translated::Unsupported,
        },
        _ => Translated::Unsupported,
    }
}

/// Combine two children under a single [`Occur`] (`Must` for `AND`, `Should` for `OR`). The
/// result is `Exact` only when both children are exact; a single `Inexact` child (or a superset
/// child produced by a negation) taints the whole combination.
fn combine(schema: &Schema, left: &Expr, right: &Expr, occur: Occur) -> Translated {
    let (lq, l_exact) = match translate(schema, left) {
        Translated::Exact(q) => (q, true),
        Translated::Inexact(q) => (q, false),
        Translated::Unsupported => return Translated::Unsupported,
    };
    let (rq, r_exact) = match translate(schema, right) {
        Translated::Exact(q) => (q, true),
        Translated::Inexact(q) => (q, false),
        Translated::Unsupported => return Translated::Unsupported,
    };

    let query: Box<dyn Query> =
        Box::new(BooleanQuery::new(vec![(occur, lq), (occur, rq)]));
    if l_exact && r_exact {
        Translated::Exact(query)
    } else {
        Translated::Inexact(query)
    }
}

fn comparison(schema: &Schema, left: &Expr, op: Operator, right: &Expr) -> Translated {
    // Normalize to `column OP literal`, flipping the operator when the literal is on the left.
    let (column, scalar, op) = match (as_column(left), as_literal(right)) {
        (Some(c), Some(s)) => (c, s, op),
        _ => match (as_literal(left), as_column(right)) {
            (Some(s), Some(c)) => (c, s, flip_op(op)),
            _ => return Translated::Unsupported,
        },
    };

    let Some((field, kind)) = classify_column(schema, column) else {
        return Translated::Unsupported;
    };

    match op {
        Operator::Eq => eq_query(field, kind, scalar),
        Operator::NotEq => match eq_query(field, kind, scalar) {
            Translated::Exact(q) | Translated::Inexact(q) => Translated::Inexact(negate(q)),
            Translated::Unsupported => Translated::Unsupported,
        },
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            range_query(field, kind, op, scalar)
        }
        _ => Translated::Unsupported,
    }
}

/// Build an equality [`TermQuery`]. Float equality is fragile (binary-representation mismatch),
/// so it is reported `Inexact` and re-checked; every other exact-eligible field is `Exact`.
fn eq_query(field: Field, kind: FieldKind, scalar: &ScalarValue) -> Translated {
    if !kind.accepts(&scalar.data_type()) {
        return Translated::Unsupported;
    }
    let Some(term) = literal_to_term(field, kind, scalar) else {
        return Translated::Unsupported;
    };
    let query: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
    if kind == FieldKind::F64 {
        Translated::Inexact(query)
    } else {
        Translated::Exact(query)
    }
}

/// Build a numeric [`RangeQuery`]. Ranges are only meaningful over numeric fields; on any other
/// field kind the comparison is not pushed. NULL rows carry no term, so they are excluded by the
/// range exactly as SQL excludes them — the range is `Exact`.
fn range_query(field: Field, kind: FieldKind, op: Operator, scalar: &ScalarValue) -> Translated {
    if !kind.is_numeric() || !kind.accepts(&scalar.data_type()) {
        return Translated::Unsupported;
    }
    let Some(term) = literal_to_term(field, kind, scalar) else {
        return Translated::Unsupported;
    };

    let (lower, upper) = match op {
        Operator::Lt => (Bound::Unbounded, Bound::Excluded(term)),
        Operator::LtEq => (Bound::Unbounded, Bound::Included(term)),
        Operator::Gt => (Bound::Excluded(term), Bound::Unbounded),
        Operator::GtEq => (Bound::Included(term), Bound::Unbounded),
        _ => return Translated::Unsupported,
    };
    Translated::Exact(Box::new(RangeQuery::new(lower, upper)))
}

fn between(
    schema: &Schema,
    expr: &Expr,
    negated: bool,
    low: &Expr,
    high: &Expr,
) -> Translated {
    let (Some(column), Some(lo), Some(hi)) = (as_column(expr), as_literal(low), as_literal(high))
    else {
        return Translated::Unsupported;
    };
    let Some((field, kind)) = classify_column(schema, column) else {
        return Translated::Unsupported;
    };
    if !kind.is_numeric() || !kind.accepts(&lo.data_type()) || !kind.accepts(&hi.data_type()) {
        return Translated::Unsupported;
    }
    let (Some(lo_term), Some(hi_term)) = (
        literal_to_term(field, kind, lo),
        literal_to_term(field, kind, hi),
    ) else {
        return Translated::Unsupported;
    };

    let query: Box<dyn Query> = Box::new(RangeQuery::new(
        Bound::Included(lo_term),
        Bound::Included(hi_term),
    ));
    if negated {
        Translated::Inexact(negate(query))
    } else {
        Translated::Exact(query)
    }
}

fn in_list(schema: &Schema, expr: &Expr, list: &[Expr], negated: bool) -> Translated {
    let Some(column) = as_column(expr) else {
        return Translated::Unsupported;
    };
    let Some((field, kind)) = classify_column(schema, column) else {
        return Translated::Unsupported;
    };

    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(list.len());
    for item in list {
        let Some(scalar) = as_literal(item) else {
            // A non-literal list entry (e.g. a subquery or column) cannot be a term.
            return Translated::Unsupported;
        };
        // `IN (.., NULL, ..)` has three-valued semantics that a term set cannot reproduce
        // (especially under negation); leave it entirely to DataFusion.
        if scalar.is_null() || !kind.accepts(&scalar.data_type()) {
            return Translated::Unsupported;
        }
        // A literal that does not fit the field's type can never equal any stored value, so
        // dropping it preserves the match set exactly (for both `IN` and `NOT IN`).
        if let Some(term) = literal_to_term(field, kind, scalar) {
            clauses.push((
                Occur::Should,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
            ));
        }
    }

    let query: Box<dyn Query> = Box::new(BooleanQuery::new(clauses));
    let exact = kind != FieldKind::F64;
    match (negated, exact) {
        // Negation adds the column's NULL rows to the match set → superset → `Inexact`.
        (true, _) => Translated::Inexact(negate(query)),
        (false, true) => Translated::Exact(query),
        (false, false) => Translated::Inexact(query),
    }
}

/// Push a prefix `LIKE 'x%'` on an untokenized string column as a lexicographic
/// [`RangeQuery`] `[prefix, prefix⁺)`. Reported `Inexact`: `LIKE` is case-sensitive over the raw
/// term and the upper bound may fall back to unbounded (a superset), so DataFusion re-checks.
fn like_prefix(schema: &Schema, like: &Like) -> Translated {
    if like.case_insensitive {
        return Translated::Unsupported;
    }
    let Some(column) = as_column(&like.expr) else {
        return Translated::Unsupported;
    };
    let Some((field, kind)) = classify_column(schema, column) else {
        return Translated::Unsupported;
    };
    if kind != FieldKind::StrExact {
        return Translated::Unsupported;
    }
    let Some(pattern) = as_literal(&like.pattern).and_then(as_utf8) else {
        return Translated::Unsupported;
    };
    let Some(prefix) = pure_prefix(pattern, like.escape_char) else {
        return Translated::Unsupported;
    };

    let lower = Bound::Included(Term::from_field_text(field, prefix));
    let upper = match prefix_upper_bound(prefix) {
        Some(upper) => Bound::Excluded(Term::from_field_text(field, upper.as_str())),
        None => Bound::Unbounded,
    };
    Translated::Inexact(Box::new(RangeQuery::new(lower, upper)))
}

/// Classify an index column by its tantivy field type, or [`None`] when the column is not in the
/// index (e.g. the synthesized `_score` column, or a base-table column that is neither a primary
/// key, a `search_field`, nor a `store_field`) or is a kind no filter can be pushed against
/// (tokenized text, bytes, dates).
fn classify_column(schema: &Schema, column: &str) -> Option<(Field, FieldKind)> {
    let field = schema.get_field(column).ok()?;
    let field_type = schema.get_field_entry(field).field_type();
    if !field_type.is_indexed() {
        return None;
    }
    let kind = match field_type {
        FieldType::I64(_) => FieldKind::I64,
        FieldType::U64(_) => FieldKind::U64,
        FieldType::F64(_) => FieldKind::F64,
        FieldType::Bool(_) => FieldKind::Bool,
        FieldType::Str(_) => {
            // A tokenized `search_field` is stemmed into multiple terms; no single term equals
            // the raw SQL string, so equality/term filters are not sound against it.
            if is_tokenized(field_type) {
                return None;
            }
            FieldKind::StrExact
        }
        // Bytes, dates, JSON, IP, and facets are not filter-pushdown targets today.
        _ => return None,
    };
    Some((field, kind))
}

/// Coerce `scalar` to the field's tantivy value type and encode it as a single [`Term`]. Reuses
/// [`array_to_terms`] so literal encoding matches the encoding used everywhere else. Returns
/// [`None`] when the literal is NULL, out of range for the target type (strict cast errors), or
/// otherwise not encodable — in which case the caller must not push the filter.
fn literal_to_term(field: Field, kind: FieldKind, scalar: &ScalarValue) -> Option<Term> {
    if scalar.is_null() {
        return None;
    }
    let coerced = scalar
        .cast_to_with_options(&kind.arrow_type(), &STRICT_CAST)
        .ok()?;
    if coerced.is_null() {
        return None;
    }
    let array = coerced.to_array().ok()?;
    let mut terms = array_to_terms(field, &array).ok()?;
    match terms.len() {
        1 => terms.pop(),
        _ => None,
    }
}

/// Wrap `query` as its logical complement: every document that does *not* match it. Combined with
/// the mandatory full-text clause above, this realizes `NOT`/`<>`/`NOT IN`/`NOT BETWEEN`. A bare
/// `MustNot` clause matches nothing in tantivy, so it is paired with an [`AllQuery`] `Must`.
fn negate(query: Box<dyn Query>) -> Box<dyn Query> {
    Box::new(BooleanQuery::new(vec![
        (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
        (Occur::MustNot, query),
    ]))
}

fn as_column(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        _ => None,
    }
}

fn as_literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(scalar, _) => Some(scalar),
        _ => None,
    }
}

fn as_utf8(scalar: &ScalarValue) -> Option<&str> {
    match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// The prefix of a `LIKE` pattern that is a pure prefix match (`'abc%'`), or [`None`] when the
/// pattern is anything else (embedded `%`/`_` wildcards, no trailing `%`, or an escape character
/// that actually appears and would change the meaning).
fn pure_prefix(pattern: &str, escape: Option<char>) -> Option<&str> {
    if let Some(escape) = escape
        && pattern.contains(escape)
    {
        return None;
    }
    let stripped = pattern.strip_suffix('%')?;
    if stripped.contains('%') || stripped.contains('_') {
        return None;
    }
    Some(stripped)
}

/// The smallest string strictly greater than every string with `prefix`, i.e. the exclusive
/// upper bound of the prefix range. [`None`] means "unbounded" (the prefix cannot be
/// incremented — e.g. it is empty or every character is the maximum code point), which yields a
/// sound superset range.
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        // Skip UTF-8 surrogate/out-of-range gaps by scanning up to the next valid code point.
        for next in (u32::from(last) + 1)..=u32::from(char::MAX) {
            if let Some(next_char) = char::from_u32(next) {
                let mut bound: String = chars.into_iter().collect();
                bound.push(next_char);
                return Some(bound);
            }
        }
        // `last` could not be incremented; drop it and carry into the previous character.
    }
    None
}

#[cfg(test)]
mod tests {
    // `Expr`, `Between`, `Like`, `ScalarValue`, and `TableProviderFilterPushDown` come in via
    // the parent module's imports.
    use super::*;
    use datafusion::prelude::{col, lit};
    use tantivy::schema::{INDEXED, STORED, STRING, Schema as TantivySchema, TEXT};

    /// An index schema exercising every classifiable field kind: signed/unsigned/float/bool
    /// numeric columns, an untokenized `STRING` column (`s`), and a tokenized text column
    /// (`body`, analyzed with a non-raw tokenizer just like a real `search_field`).
    fn schema() -> TantivySchema {
        let mut builder = TantivySchema::builder();
        builder.add_i64_field("i", INDEXED | STORED);
        builder.add_u64_field("u", INDEXED | STORED);
        builder.add_f64_field("f", INDEXED | STORED);
        builder.add_bool_field("b", INDEXED | STORED);
        builder.add_text_field("s", STRING | STORED);
        builder.add_text_field("body", TEXT | STORED);
        builder.build()
    }

    fn classify(expr: &Expr) -> TableProviderFilterPushDown {
        classify_filter(&schema(), expr)
    }

    fn assert_exact(expr: &Expr) {
        assert!(
            matches!(classify(expr), TableProviderFilterPushDown::Exact),
            "expected Exact for {expr:?}, got {:?}",
            classify(expr)
        );
        assert!(
            translate_filter(&schema(), expr).is_some(),
            "Exact filter must translate: {expr:?}"
        );
    }

    fn assert_inexact(expr: &Expr) {
        assert!(
            matches!(classify(expr), TableProviderFilterPushDown::Inexact),
            "expected Inexact for {expr:?}, got {:?}",
            classify(expr)
        );
        assert!(
            translate_filter(&schema(), expr).is_some(),
            "Inexact filter must translate: {expr:?}"
        );
    }

    fn assert_unsupported(expr: &Expr) {
        assert!(
            matches!(classify(expr), TableProviderFilterPushDown::Unsupported),
            "expected Unsupported for {expr:?}, got {:?}",
            classify(expr)
        );
        assert!(
            translate_filter(&schema(), expr).is_none(),
            "Unsupported filter must not translate: {expr:?}"
        );
    }

    #[test]
    fn eq_on_indexed_scalars_is_exact() {
        assert_exact(&col("i").eq(lit(5_i64)));
        assert_exact(&col("u").eq(lit(5_i64))); // non-negative int literal fits u64
        assert_exact(&col("b").eq(lit(true)));
        assert_exact(&col("s").eq(lit("hello")));
        // Literal on the left side is normalized to `column op literal`.
        assert_exact(&lit(5_i64).eq(col("i")));
    }

    #[test]
    fn eq_on_float_is_inexact() {
        assert_inexact(&col("f").eq(lit(1.5_f64)));
    }

    #[test]
    fn eq_on_tokenized_text_is_unsupported() {
        assert_unsupported(&col("body").eq(lit("hello")));
    }

    #[test]
    fn out_of_range_literal_is_not_pushed() {
        // -5 cannot be a u64: strict cast fails, so the filter is not pushed.
        assert_unsupported(&col("u").eq(lit(-5_i64)));
        assert_unsupported(&col("u").gt(lit(-5_i64)));
    }

    #[test]
    fn numeric_ranges_are_exact() {
        assert_exact(&col("i").lt(lit(5_i64)));
        assert_exact(&col("i").lt_eq(lit(5_i64)));
        assert_exact(&col("i").gt(lit(5_i64)));
        assert_exact(&col("i").gt_eq(lit(5_i64)));
        assert_exact(&col("f").gt(lit(1.5_f64))); // float *ranges* are exact; only float `=` is inexact
        assert_exact(&col("u").lt(lit(100_i64)));
    }

    #[test]
    fn ranges_on_non_numeric_are_unsupported() {
        assert_unsupported(&col("s").lt(lit("m")));
        assert_unsupported(&col("b").gt(lit(false)));
    }

    #[test]
    fn between_is_exact_and_negated_is_inexact() {
        let between = Expr::Between(Between::new(
            Box::new(col("i")),
            false,
            Box::new(lit(1_i64)),
            Box::new(lit(10_i64)),
        ));
        assert_exact(&between);

        let negated = Expr::Between(Between::new(
            Box::new(col("i")),
            true,
            Box::new(lit(1_i64)),
            Box::new(lit(10_i64)),
        ));
        assert_inexact(&negated);
    }

    #[test]
    fn in_list_classification() {
        assert_exact(&col("i").in_list(vec![lit(1_i64), lit(2_i64), lit(3_i64)], false));
        assert_exact(&col("s").in_list(vec![lit("a"), lit("b")], false));
        // Negated IN adds NULL rows to the match set (superset), so it is Inexact.
        assert_inexact(&col("i").in_list(vec![lit(1_i64), lit(2_i64)], true));
        // Float IN is Inexact for the same reason `=` on a float is.
        assert_inexact(&col("f").in_list(vec![lit(1.5_f64)], false));
        // A NULL in the list has three-valued semantics a term set cannot reproduce.
        assert_unsupported(&col("i").in_list(vec![lit(1_i64), lit(ScalarValue::Int64(None))], false));
    }

    #[test]
    fn prefix_like_on_string_is_inexact() {
        let like = Expr::Like(Like::new(
            false,
            Box::new(col("s")),
            Box::new(lit("abc%")),
            None,
            false,
        ));
        assert_inexact(&like);
    }

    #[test]
    fn non_prefix_or_ilike_is_unsupported() {
        // Embedded wildcard, not a pure prefix.
        let infix = Expr::Like(Like::new(
            false,
            Box::new(col("s")),
            Box::new(lit("a%b")),
            None,
            false,
        ));
        assert_unsupported(&infix);
        // Case-insensitive (ILIKE) cannot match a case-sensitive raw term soundly.
        let ilike = Expr::Like(Like::new(
            false,
            Box::new(col("s")),
            Box::new(lit("abc%")),
            None,
            true,
        ));
        assert_unsupported(&ilike);
        // LIKE against a tokenized column is unsupported regardless of shape.
        let tokenized = Expr::Like(Like::new(
            false,
            Box::new(col("body")),
            Box::new(lit("abc%")),
            None,
            false,
        ));
        assert_unsupported(&tokenized);
    }

    #[test]
    fn boolean_combinations() {
        // AND/OR of two Exact children is Exact.
        assert_exact(&col("i").eq(lit(1_i64)).and(col("s").eq(lit("x"))));
        assert_exact(&col("i").eq(lit(1_i64)).or(col("u").eq(lit(2_i64))));
        // An Inexact child taints the whole combination.
        assert_inexact(&col("i").eq(lit(1_i64)).and(col("f").eq(lit(1.5_f64))));
        assert_inexact(&col("f").eq(lit(1.5_f64)).or(col("i").eq(lit(1_i64))));
        // An Unsupported child makes the whole combination Unsupported.
        assert_unsupported(&col("i").eq(lit(1_i64)).and(col("body").eq(lit("x"))));
        assert_unsupported(&col("i").eq(lit(1_i64)).or(col("i").is_null()));
    }

    #[test]
    fn negation_is_inexact() {
        // NOT and `<>` both admit NULL rows, so they are Inexact even over exact fields.
        assert_inexact(&Expr::Not(Box::new(col("i").eq(lit(5_i64)))));
        assert_inexact(&col("i").not_eq(lit(5_i64)));
        assert_inexact(&col("s").not_eq(lit("x")));
        // NOT of an unsupported inner is still unsupported.
        assert_unsupported(&Expr::Not(Box::new(col("body").eq(lit("x")))));
    }

    #[test]
    fn null_checks_and_unknown_columns_are_unsupported() {
        assert_unsupported(&col("i").is_null());
        assert_unsupported(&col("i").is_not_null());
        assert_unsupported(&col("does_not_exist").eq(lit(5_i64)));
        // The synthesized score column is not in the tantivy schema.
        assert_unsupported(&col("_score").gt(lit(0.5_f64)));
    }
}
