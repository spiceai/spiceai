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

//! The single translation pass shared by [`classify_filter`] and [`translate_filter`].

use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use serde_json::{Value, json};

use crate::schema::{EsFieldType, EsFilterSchema};

/// The outcome of translating one `DataFusion` [`Expr`] into an Elasticsearch query clause.
///
/// `Pushable { exact: true, .. }` means the emitted clause matches the SQL predicate *exactly*
/// under Elasticsearch semantics; `exact: false` means the clause matches a *superset* of the
/// predicate (`DataFusion` re-checks it above the scan). The clause is NEVER a subset — that would
/// silently drop matching rows. `Unsupported` means no clause is emitted and `DataFusion` handles
/// the predicate entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Pushable { exact: bool, clause: Value },
    Unsupported,
}

impl Outcome {
    fn inexact(clause: Value) -> Self {
        Outcome::Pushable {
            exact: false,
            clause,
        }
    }
}

/// Classify how a single filter can be pushed into Elasticsearch.
///
/// `Exact` ⟹ the Elasticsearch query matches the SQL predicate exactly; `Inexact` ⟹ it matches a
/// superset and `DataFusion` re-checks; `Unsupported` ⟹ `DataFusion` evaluates it entirely.
#[must_use]
pub fn classify_filter(schema: &EsFilterSchema, filter: &Expr) -> TableProviderFilterPushDown {
    match translate(schema, filter) {
        Outcome::Pushable { exact: true, .. } => TableProviderFilterPushDown::Exact,
        Outcome::Pushable { exact: false, .. } => TableProviderFilterPushDown::Inexact,
        Outcome::Unsupported => TableProviderFilterPushDown::Unsupported,
    }
}

/// Translate a single filter into the Elasticsearch query DSL clause (to be placed in a
/// `bool.filter` context), or `None` when the filter is not pushable.
///
/// Consistent with [`classify_filter`] by construction: both are views over the same pass, so a
/// filter classified `Exact`/`Inexact` always yields `Some`, and `Unsupported` always yields
/// `None`.
#[must_use]
pub fn translate_filter(schema: &EsFilterSchema, filter: &Expr) -> Option<Value> {
    match translate(schema, filter) {
        Outcome::Pushable { clause, .. } => Some(clause),
        Outcome::Unsupported => None,
    }
}

fn translate(schema: &EsFilterSchema, expr: &Expr) -> Outcome {
    match unwrap_alias(expr) {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And => translate_and(schema, &binary.left, &binary.right),
            Operator::Or => translate_or(schema, &binary.left, &binary.right),
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                translate_comparison(schema, &binary.left, binary.op, &binary.right)
            }
            _ => Outcome::Unsupported,
        },
        Expr::InList(in_list) => translate_in_list(schema, in_list),
        Expr::Between(between) => translate_between(schema, between),
        Expr::IsNull(inner) => translate_is_null(schema, inner, true),
        Expr::IsNotNull(inner) => translate_is_null(schema, inner, false),
        Expr::Like(like) => translate_like(schema, like),
        Expr::Not(inner) => translate_not(schema, inner),
        _ => Outcome::Unsupported,
    }
}

// ── Comparison (=, <>, <, <=, >, >=) ─────────────────────────────────────────

fn translate_comparison(
    schema: &EsFilterSchema,
    left: &Expr,
    op: Operator,
    right: &Expr,
) -> Outcome {
    // Normalize to (column, operator-relative-to-column, literal), handling either operand order.
    let (column, op, scalar) = if let (Some(col), Some(lit)) = (as_column(left), as_literal(right))
    {
        (col, op, lit)
    } else if let (Some(col), Some(lit)) = (as_column(right), as_literal(left)) {
        (col, flip_operator(op), lit)
    } else {
        return Outcome::Unsupported;
    };

    let Some(field_type) = schema.get(column) else {
        return Outcome::Unsupported;
    };
    let Some(value) = scalar_to_json(scalar) else {
        return Outcome::Unsupported;
    };
    if !value_matches_field(field_type, &value) {
        return Outcome::Unsupported;
    }

    match op {
        Operator::Eq => translate_eq(schema, field_type, column, &value),
        Operator::NotEq => negate_if_exact(translate_eq(schema, field_type, column, &value)),
        Operator::Lt => translate_range(schema, field_type, column, "lt", &value),
        Operator::LtEq => translate_range(schema, field_type, column, "lte", &value),
        Operator::Gt => translate_range(schema, field_type, column, "gt", &value),
        Operator::GtEq => translate_range(schema, field_type, column, "gte", &value),
        _ => Outcome::Unsupported,
    }
}

fn translate_eq(
    schema: &EsFilterSchema,
    field_type: &EsFieldType,
    column: &str,
    value: &Value,
) -> Outcome {
    // A literal longer than a `TextWithKeyword`'s `ignore_above` can never be indexed in the
    // sub-field, so pushing `term` for it would silently exclude a row whose real value equals
    // the literal — a subset, not a superset.
    if !field_type.accepts_value_length(value) {
        return Outcome::Unsupported;
    }
    let field = field_type.value_field(column);
    // `is_confirmed_scalar` caps exactness for a field whose cardinality Elasticsearch's mapping
    // cannot confirm — see `EsFilterSchema::is_confirmed_scalar`.
    let exact = field_type.is_exact_for_value_match() && schema.is_confirmed_scalar(column);
    Outcome::Pushable {
        exact,
        clause: json!({ "term": { field: value.clone() } }),
    }
}

fn translate_range(
    schema: &EsFilterSchema,
    field_type: &EsFieldType,
    column: &str,
    es_op: &str,
    value: &Value,
) -> Outcome {
    // No safe superset for a range on a boolean field, a quantized float, or a keyword-family
    // field over its `ignore_above` limit (see `EsFieldType::supports_range`); nor for a field
    // Elasticsearch has no `doc_values` for.
    if !field_type.supports_range() || !schema.has_doc_values(column) {
        return Outcome::Unsupported;
    }
    let field = field_type.value_field(column);
    // Only integers compare exactly, and only when the field is confirmed scalar (see
    // `translate_eq`). Float representation and keyword collation can diverge from SQL ordering,
    // so those are re-checked above the scan.
    let exact = matches!(field_type, EsFieldType::Integer) && schema.is_confirmed_scalar(column);
    Outcome::Pushable {
        exact,
        clause: json!({ "range": { field: { es_op: value.clone() } } }),
    }
}

// ── IN / NOT IN ──────────────────────────────────────────────────────────────

fn translate_in_list(
    schema: &EsFilterSchema,
    in_list: &datafusion::logical_expr::expr::InList,
) -> Outcome {
    let Some(column) = as_column(&in_list.expr) else {
        return Outcome::Unsupported;
    };
    let Some(field_type) = schema.get(column) else {
        return Outcome::Unsupported;
    };

    let mut values = Vec::with_capacity(in_list.list.len());
    for item in &in_list.list {
        let Some(scalar) = as_literal(item) else {
            return Outcome::Unsupported;
        };
        let Some(value) = scalar_to_json(scalar) else {
            return Outcome::Unsupported;
        };
        if !value_matches_field(field_type, &value) {
            return Outcome::Unsupported;
        }
        // A single over-length literal (see `translate_eq`) makes the *whole* IN-list
        // unsafe to push: dropping just that literal from `terms` would still exclude a row
        // matching it via that branch, which no re-check above the scan can recover.
        if !field_type.accepts_value_length(&value) {
            return Outcome::Unsupported;
        }
        values.push(value);
    }
    if values.is_empty() {
        return Outcome::Unsupported;
    }

    let field = field_type.value_field(column);
    // See `translate_eq` on why exactness is also capped by `is_confirmed_scalar`.
    let exact = field_type.is_exact_for_value_match() && schema.is_confirmed_scalar(column);
    let terms = Outcome::Pushable {
        exact,
        clause: json!({ "terms": { field: values } }),
    };
    if in_list.negated {
        negate_if_exact(terms)
    } else {
        terms
    }
}

// ── BETWEEN / NOT BETWEEN ─────────────────────────────────────────────────────

fn translate_between(
    schema: &EsFilterSchema,
    between: &datafusion::logical_expr::expr::Between,
) -> Outcome {
    let Some(column) = as_column(&between.expr) else {
        return Outcome::Unsupported;
    };
    let (Some(low), Some(high)) = (as_literal(&between.low), as_literal(&between.high)) else {
        return Outcome::Unsupported;
    };
    let Some(field_type) = schema.get(column) else {
        return Outcome::Unsupported;
    };
    // See `translate_range` on `supports_range` and `has_doc_values`.
    if !field_type.supports_range() || !schema.has_doc_values(column) {
        return Outcome::Unsupported;
    }
    let (Some(low), Some(high)) = (scalar_to_json(low), scalar_to_json(high)) else {
        return Outcome::Unsupported;
    };
    if !value_matches_field(field_type, &low) || !value_matches_field(field_type, &high) {
        return Outcome::Unsupported;
    }

    let field = field_type.value_field(column);
    // See `translate_eq` on why exactness is also capped by `is_confirmed_scalar`.
    let exact = matches!(field_type, EsFieldType::Integer) && schema.is_confirmed_scalar(column);
    let range = Outcome::Pushable {
        exact,
        clause: json!({ "range": { field: { "gte": low, "lte": high } } }),
    };
    if between.negated {
        negate_if_exact(range)
    } else {
        range
    }
}

// ── IS NULL / IS NOT NULL ─────────────────────────────────────────────────────

fn translate_is_null(schema: &EsFilterSchema, inner: &Expr, is_null: bool) -> Outcome {
    let Some(column) = as_column(inner) else {
        return Outcome::Unsupported;
    };
    if schema.get(column).is_none() {
        return Outcome::Unsupported;
    }
    // A `null_value` sentinel makes a source `null` still "exist" in the index — `exists` can't
    // tell it apart from a real value, so `must_not exists` (IS NULL) would wrongly exclude that
    // row from the pre-filtered candidates, which no above-scan recheck can restore.
    if !schema.supports_null_check(column) {
        return Outcome::Unsupported;
    }
    let exists = json!({ "exists": { "field": column } });
    // SQL NULL semantics and Elasticsearch's "field has no non-null value" differ at the edges
    // (arrays, explicit JSON null), so these are re-checked above the scan.
    if is_null {
        Outcome::inexact(json!({ "bool": { "must_not": [exists] } }))
    } else {
        Outcome::inexact(exists)
    }
}

// ── LIKE 'prefix%' ─────────────────────────────────────────────────────────────

fn translate_like(schema: &EsFilterSchema, like: &datafusion::logical_expr::expr::Like) -> Outcome {
    // A negated or case-insensitive LIKE, or a `prefix` (always a superset) under NOT, would
    // require an exact base clause to negate safely — none is available.
    if like.negated || like.case_insensitive {
        return Outcome::Unsupported;
    }
    let Some(column) = as_column(&like.expr) else {
        return Outcome::Unsupported;
    };
    let Some(field_type) = schema.get(column) else {
        return Outcome::Unsupported;
    };
    if !field_type.supports_prefix() {
        return Outcome::Unsupported;
    }
    let Some(pattern) = as_literal(&like.pattern).and_then(scalar_string) else {
        return Outcome::Unsupported;
    };
    let Some(prefix) = prefix_from_like_pattern(&pattern, like.escape_char) else {
        return Outcome::Unsupported;
    };

    let field = field_type.value_field(column);
    // `prefix` is case- and analysis-sensitive; `DataFusion` re-checks.
    Outcome::inexact(json!({ "prefix": { field: prefix } }))
}

/// Extract the literal prefix of a `LIKE` pattern of the form `abc%` — a run of non-wildcard
/// characters followed by a single trailing `%`, with no `_` and no interior `%`. Returns `None`
/// for any other shape (including patterns using the escape character, which are not modeled).
fn prefix_from_like_pattern(pattern: &str, escape_char: Option<char>) -> Option<String> {
    if escape_char.is_some() && pattern.contains(escape_char?) {
        return None;
    }
    let stripped = pattern.strip_suffix('%')?;
    if stripped.is_empty() || stripped.contains('%') || stripped.contains('_') {
        return None;
    }
    Some(stripped.to_string())
}

// ── NOT ────────────────────────────────────────────────────────────────────────

fn translate_not(schema: &EsFilterSchema, inner: &Expr) -> Outcome {
    negate_if_exact(translate(schema, inner))
}

/// Wrap a clause in `bool.must_not`, but only when the inner clause matches the predicate
/// *exactly*. Negating a superset clause would yield a subset — dropping matching rows — so an
/// inexact or unsupported inner clause makes the negation unsupported. The negation itself is
/// inexact because `must_not` also matches documents missing the field, which SQL `NOT`/`<>`
/// excludes (they evaluate to NULL).
fn negate_if_exact(inner: Outcome) -> Outcome {
    match inner {
        Outcome::Pushable {
            exact: true,
            clause,
        } => Outcome::inexact(json!({ "bool": { "must_not": [clause] } })),
        _ => Outcome::Unsupported,
    }
}

// ── AND / OR ─────────────────────────────────────────────────────────────────

fn translate_and(schema: &EsFilterSchema, left: &Expr, right: &Expr) -> Outcome {
    match (translate(schema, left), translate(schema, right)) {
        (
            Outcome::Pushable {
                exact: le,
                clause: lc,
            },
            Outcome::Pushable {
                exact: re,
                clause: rc,
            },
        ) => Outcome::Pushable {
            exact: le && re,
            clause: json!({ "bool": { "filter": [lc, rc] } }),
        },
        // Pushing one conjunct alone yields a superset of `a AND b`; `DataFusion` re-checks the
        // dropped conjunct, so it is safe but inexact.
        (Outcome::Pushable { clause, .. }, Outcome::Unsupported)
        | (Outcome::Unsupported, Outcome::Pushable { clause, .. }) => Outcome::inexact(clause),
        (Outcome::Unsupported, Outcome::Unsupported) => Outcome::Unsupported,
    }
}

fn translate_or(schema: &EsFilterSchema, left: &Expr, right: &Expr) -> Outcome {
    match (translate(schema, left), translate(schema, right)) {
        (
            Outcome::Pushable {
                exact: le,
                clause: lc,
            },
            Outcome::Pushable {
                exact: re,
                clause: rc,
            },
        ) => Outcome::Pushable {
            exact: le && re,
            clause: json!({ "bool": { "should": [lc, rc], "minimum_should_match": 1 } }),
        },
        // One disjunct alone is a *subset* of `a OR b`, which would drop rows — never push a
        // partial OR.
        _ => Outcome::Unsupported,
    }
}

// ── Expression / literal helpers ──────────────────────────────────────────────

fn unwrap_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unwrap_alias(&alias.expr),
        other => other,
    }
}

fn as_column(expr: &Expr) -> Option<&str> {
    match unwrap_alias(expr) {
        Expr::Column(col) => Some(col.name.as_str()),
        _ => None,
    }
}

fn as_literal(expr: &Expr) -> Option<&ScalarValue> {
    match unwrap_alias(expr) {
        Expr::Literal(value, _) => Some(value),
        _ => None,
    }
}

fn scalar_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

fn flip_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// Whether a JSON literal is compatible with a field's Elasticsearch type. Guards against pushing
/// a type-mismatched predicate (e.g. a string literal against a numeric field) that Elasticsearch
/// would reject or coerce unexpectedly.
fn value_matches_field(field_type: &EsFieldType, value: &Value) -> bool {
    match field_type {
        EsFieldType::Boolean => value.is_boolean(),
        EsFieldType::Integer => value.is_i64() || value.is_u64(),
        EsFieldType::Float | EsFieldType::QuantizedFloat => value.is_number(),
        EsFieldType::Keyword { .. } | EsFieldType::TextWithKeyword { .. } => value.is_string(),
    }
}

/// Convert a non-NULL `DataFusion` [`ScalarValue`] into a JSON value for a `term`/`range`/`terms`
/// clause. Returns `None` for NULL scalars and for types this translator does not model (dates,
/// timestamps, decimals, nested types).
fn scalar_to_json(value: &ScalarValue) -> Option<Value> {
    match value {
        ScalarValue::Boolean(Some(b)) => Some(Value::Bool(*b)),
        ScalarValue::Int8(Some(v)) => Some(json!(v)),
        ScalarValue::Int16(Some(v)) => Some(json!(v)),
        ScalarValue::Int32(Some(v)) => Some(json!(v)),
        ScalarValue::Int64(Some(v)) => Some(json!(v)),
        ScalarValue::UInt8(Some(v)) => Some(json!(v)),
        ScalarValue::UInt16(Some(v)) => Some(json!(v)),
        ScalarValue::UInt32(Some(v)) => Some(json!(v)),
        ScalarValue::UInt64(Some(v)) => Some(json!(v)),
        ScalarValue::Float32(Some(v)) => Some(json!(v)),
        ScalarValue::Float64(Some(v)) => Some(json!(v)),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(Value::String(s.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    fn schema() -> EsFilterSchema {
        EsFilterSchema::new()
            .with_field("age", EsFieldType::Integer)
            .with_field("score", EsFieldType::Float)
            .with_field("weight", EsFieldType::QuantizedFloat)
            .with_field("active", EsFieldType::Boolean)
            .with_field(
                "status",
                EsFieldType::Keyword {
                    ignore_above: None,
                    has_normalizer: false,
                },
            )
            .with_field(
                "title",
                EsFieldType::TextWithKeyword {
                    keyword_subfield: "keyword".to_string(),
                    ignore_above: Some(8),
                    has_normalizer: false,
                },
            )
    }

    fn classify(expr: &Expr) -> TableProviderFilterPushDown {
        classify_filter(&schema(), expr)
    }

    fn clause(expr: &Expr) -> Value {
        translate_filter(&schema(), expr).expect("expected a pushable clause")
    }

    #[test]
    fn integer_eq_is_exact_term() {
        let expr = col("age").eq(lit(30_i64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Exact);
        assert_eq!(clause(&expr), json!({ "term": { "age": 30 } }));
    }

    #[test]
    fn eq_operand_order_is_normalized() {
        let expr = lit(30_i64).eq(col("age"));
        assert_eq!(clause(&expr), json!({ "term": { "age": 30 } }));
    }

    #[test]
    fn boolean_eq_is_exact_term() {
        let expr = col("active").eq(lit(true));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Exact);
        assert_eq!(clause(&expr), json!({ "term": { "active": true } }));
    }

    #[test]
    fn keyword_eq_is_exact_term() {
        let expr = col("status").eq(lit("open"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Exact);
        assert_eq!(clause(&expr), json!({ "term": { "status": "open" } }));
    }

    #[test]
    fn float_eq_is_inexact_due_to_representation() {
        let expr = col("score").eq(lit(1.5_f64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(clause(&expr), json!({ "term": { "score": 1.5 } }));
    }

    #[test]
    fn text_eq_targets_keyword_subfield_inexact() {
        let expr = col("title").eq(lit("hello"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(
            clause(&expr),
            json!({ "term": { "title.keyword": "hello" } })
        );
    }

    #[test]
    fn unknown_column_is_unsupported() {
        let expr = col("unmapped").eq(lit(1_i64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
        assert_eq!(translate_filter(&schema(), &expr), None);
    }

    #[test]
    fn type_mismatched_literal_is_unsupported() {
        let expr = col("age").eq(lit("not-a-number"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn integer_range_is_exact() {
        let expr = col("age").gt(lit(18_i64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Exact);
        assert_eq!(clause(&expr), json!({ "range": { "age": { "gt": 18 } } }));
    }

    #[test]
    fn reversed_range_flips_operator() {
        // `18 < age` means `age > 18`.
        let expr = lit(18_i64).lt(col("age"));
        assert_eq!(clause(&expr), json!({ "range": { "age": { "gt": 18 } } }));
    }

    #[test]
    fn float_range_is_inexact() {
        let expr = col("score").lt_eq(lit(9.9_f64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(
            clause(&expr),
            json!({ "range": { "score": { "lte": 9.9 } } })
        );
    }

    #[test]
    fn in_list_is_terms() {
        let expr = col("status").in_list(vec![lit("a"), lit("b")], false);
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Exact);
        assert_eq!(clause(&expr), json!({ "terms": { "status": ["a", "b"] } }));
    }

    #[test]
    fn not_in_list_on_exact_field_is_inexact_must_not() {
        let expr = col("status").in_list(vec![lit("a")], true);
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(
            clause(&expr),
            json!({ "bool": { "must_not": [{ "terms": { "status": ["a"] } }] } })
        );
    }

    #[test]
    fn not_in_list_on_inexact_field_is_unsupported() {
        // A NOT IN over a text/keyword-subfield base (inexact) cannot be negated safely.
        let expr = col("title").in_list(vec![lit("a")], true);
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn between_integer_is_exact_range() {
        let expr = Expr::Between(datafusion::logical_expr::expr::Between::new(
            Box::new(col("age")),
            false,
            Box::new(lit(10_i64)),
            Box::new(lit(20_i64)),
        ));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Exact);
        assert_eq!(
            clause(&expr),
            json!({ "range": { "age": { "gte": 10, "lte": 20 } } })
        );
    }

    #[test]
    fn is_null_is_inexact_must_not_exists() {
        let expr = col("status").is_null();
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(
            clause(&expr),
            json!({ "bool": { "must_not": [{ "exists": { "field": "status" } }] } })
        );
    }

    #[test]
    fn is_not_null_is_inexact_exists() {
        let expr = col("status").is_not_null();
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(clause(&expr), json!({ "exists": { "field": "status" } }));
    }

    #[test]
    fn is_null_on_field_with_null_value_is_unsupported() {
        // A `null_value` sentinel means a source `null` still "exists" in the index, so
        // `exists`/`must_not exists` can't be trusted to mean IS [NOT] NULL.
        let info = crate::schema::EsMappingField {
            field_type: "keyword".to_string(),
            keyword_subfield: None,
            keyword_ignore_above: None,
            has_null_value: true,
            indexed: true,
            has_doc_values: true,
            has_normalizer: false,
        };
        let schema = EsFilterSchema::from_mapping([("code", &info)]);
        assert_eq!(
            classify_filter(&schema, &col("code").is_null()),
            TableProviderFilterPushDown::Unsupported
        );
        assert_eq!(
            classify_filter(&schema, &col("code").is_not_null()),
            TableProviderFilterPushDown::Unsupported
        );
        // `null_value` doesn't change term-query correctness, but a mapping-derived field's
        // cardinality is never confirmed scalar (see `EsFilterSchema::is_confirmed_scalar`), so
        // equality is capped to `Inexact` regardless.
        assert_eq!(
            classify_filter(&schema, &col("code").eq(lit("open"))),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn prefix_like_on_keyword_is_inexact_prefix() {
        let expr = col("status").like(lit("open%"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(clause(&expr), json!({ "prefix": { "status": "open" } }));
    }

    #[test]
    fn prefix_like_on_text_with_keyword_is_unsupported() {
        // `ignore_above` means an arbitrarily long matching value could be entirely absent from
        // the `.keyword` sub-field, so a prefix clause is never a safe superset here.
        let expr = col("title").like(lit("hel%"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
        assert_eq!(translate_filter(&schema(), &expr), None);
    }

    #[test]
    fn eq_literal_over_ignore_above_is_unsupported() {
        // "title" has ignore_above: 8; a 9-character literal can never be indexed in the
        // sub-field, so pushing `term` for it would silently drop a matching row.
        let expr = col("title").eq(lit("123456789"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn eq_literal_within_ignore_above_is_still_pushed() {
        let expr = col("title").eq(lit("12345678"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn in_list_with_one_over_length_literal_is_unsupported() {
        // Dropping just the over-length literal would still miss a row matching it.
        let expr = col("title").in_list(vec![lit("short"), lit("123456789")], false);
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn range_on_keyword_with_ignore_above_is_unsupported() {
        // A value over "title"'s ignore_above: 8 has no entry in the sub-field at all, so no
        // range boundary — regardless of the literal — can be trusted not to exclude a row that
        // truly satisfies the predicate.
        let expr = col("title").gt(lit("abc"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
        assert_eq!(translate_filter(&schema(), &expr), None);
    }

    /// A schema with a `normalizer` configured on both a bare `keyword` field and a `text` field's
    /// `keyword` sub-field, mirroring an externally-managed mapping with e.g. a `lowercase`
    /// normalizer.
    fn normalizer_schema() -> EsFilterSchema {
        EsFilterSchema::new()
            .with_field(
                "code",
                EsFieldType::Keyword {
                    ignore_above: None,
                    has_normalizer: true,
                },
            )
            .with_field(
                "name",
                EsFieldType::TextWithKeyword {
                    keyword_subfield: "keyword".to_string(),
                    ignore_above: None,
                    has_normalizer: true,
                },
            )
    }

    /// Normalization is not order-preserving (e.g. a lowercase normalizer indexes `"Z"` before
    /// `"a"` even though the raw `_source` values compare the other way around), so a range/
    /// `BETWEEN` clause against a normalized `keyword`-family field is not provably a superset of
    /// the SQL predicate and must not be pushed. Equality/`IN` still push (Elasticsearch's `term`
    /// query is a safe superset — it can only match extra rows, never miss one), but are `Inexact`
    /// rather than `Exact`: a `term` match against the normalized indexed value can include a
    /// source row that does not equal the raw SQL literal (e.g. a lowercase normalizer makes
    /// `"ABC"` match a `term` query for `"abc"`), so `DataFusion` must re-check.
    #[test]
    fn normalizer_disables_range_and_exact_equality_but_not_in_or_eq_pushdown() {
        let schema = normalizer_schema();

        for column in ["code", "name"] {
            assert_eq!(
                classify_filter(&schema, &col(column).gt(lit("a"))),
                TableProviderFilterPushDown::Unsupported,
                "range on a normalized {column} field must be unsupported"
            );
            assert_eq!(
                classify_filter(
                    &schema,
                    &Expr::Between(datafusion::logical_expr::expr::Between::new(
                        Box::new(col(column)),
                        false,
                        Box::new(lit("a")),
                        Box::new(lit("z")),
                    ))
                ),
                TableProviderFilterPushDown::Unsupported,
                "BETWEEN on a normalized {column} field must be unsupported"
            );
        }

        // Equality/IN still push against the bare keyword field, but as `Inexact`: a `term` match
        // against the normalized indexed value can include a source row that doesn't equal the
        // raw SQL literal.
        assert_eq!(
            classify_filter(&schema, &col("code").eq(lit("abc"))),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            classify_filter(
                &schema,
                &col("code").in_list(vec![lit("a"), lit("b")], false)
            ),
            TableProviderFilterPushDown::Inexact
        );

        // Equality/IN against the text-with-keyword field stay `Inexact`, same as without a
        // normalizer — analysis/collation differences already require a `DataFusion` re-check.
        assert_eq!(
            classify_filter(&schema, &col("name").eq(lit("abc"))),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            classify_filter(
                &schema,
                &col("name").in_list(vec![lit("a"), lit("b")], false)
            ),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn quantized_float_eq_is_inexact() {
        // Equality applies the same quantization to the query literal on both sides.
        let expr = col("weight").eq(lit(1.04_f64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn quantized_float_range_is_unsupported() {
        // A range boundary compares against the quantized indexed value and can exclude a row
        // that satisfies the SQL predicate on the unquantized source value.
        let expr = col("weight").gt(lit(1.01_f64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn quantized_float_between_is_unsupported() {
        let expr = Expr::Between(datafusion::logical_expr::expr::Between::new(
            Box::new(col("weight")),
            false,
            Box::new(lit(1.0_f64)),
            Box::new(lit(2.0_f64)),
        ));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn like_with_interior_wildcard_is_unsupported() {
        let expr = col("status").like(lit("a%b%"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn like_with_underscore_is_unsupported() {
        let expr = col("status").like(lit("a_c%"));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn not_eq_on_exact_field_is_inexact_must_not() {
        let expr = col("age").not_eq(lit(5_i64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(
            clause(&expr),
            json!({ "bool": { "must_not": [{ "term": { "age": 5 } }] } })
        );
    }

    #[test]
    fn not_eq_on_float_is_unsupported() {
        // Float `=` is inexact, so its negation would be a subset — not pushable.
        let expr = col("score").not_eq(lit(1.0_f64));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn and_of_two_exact_is_exact_filter() {
        let expr = col("age")
            .gt(lit(18_i64))
            .and(col("status").eq(lit("open")));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Exact);
        assert_eq!(
            clause(&expr),
            json!({ "bool": { "filter": [
                { "range": { "age": { "gt": 18 } } },
                { "term": { "status": "open" } }
            ] } })
        );
    }

    #[test]
    fn and_with_one_unsupported_pushes_the_other_inexact() {
        let expr = col("age")
            .gt(lit(18_i64))
            .and(col("unmapped").eq(lit(1_i64)));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
        assert_eq!(clause(&expr), json!({ "range": { "age": { "gt": 18 } } }));
    }

    #[test]
    fn or_of_two_exact_is_exact_should() {
        let expr = col("age").eq(lit(1_i64)).or(col("age").eq(lit(2_i64)));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Exact);
        assert_eq!(
            clause(&expr),
            json!({ "bool": {
                "should": [{ "term": { "age": 1 } }, { "term": { "age": 2 } }],
                "minimum_should_match": 1
            } })
        );
    }

    #[test]
    fn or_with_one_unsupported_is_unsupported() {
        // A partial OR would drop rows, so the whole disjunction is not pushable.
        let expr = col("age").eq(lit(1_i64)).or(col("unmapped").eq(lit(2_i64)));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Unsupported);
        assert_eq!(translate_filter(&schema(), &expr), None);
    }

    #[test]
    fn or_mixing_exact_and_inexact_is_inexact() {
        let expr = col("age").eq(lit(1_i64)).or(col("score").eq(lit(2.0_f64)));
        assert_eq!(classify(&expr), TableProviderFilterPushDown::Inexact);
    }
}
