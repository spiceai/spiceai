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

//! `BigQuery` translations for the JSON extraction functions, and the
//! `BigQuery` dialect that installs them.
//!
//! `datafusion-functions-json` takes a **variadic path**, not a `JSONPath`
//! string: `json_get_int(col, 'a', 'b', 0)` reads key `a`, then key `b`, then
//! array element 0. A string argument is always an object key — `('a.b')` is
//! one key literally named `a.b`, not a two-step path — and an integer argument
//! is always an array index. [`json_path`] is the one place that mapping is
//! written down, and both the translation and the pushdown policy read it, so
//! what the dialect can render and what the deny-list lets through cannot
//! diverge.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::sql::sqlparser::ast::{
    self, BinaryOperator, Function, FunctionArg, FunctionArgExpr, ObjectName, WindowFrameBound,
};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{
    BigQueryDialect, CharacterLengthStyle, DateFieldExtractStyle, Dialect, IntervalStyle,
    ScalarFnToSqlHandler,
};

pub(crate) const JSON_GET_INT_NAME: &str = "json_get_int";
pub(crate) const JSON_GET_FLOAT_NAME: &str = "json_get_float";

/// The grammar Rust's `i64::FromStr` accepts, which is what
/// `json_get_int` applies to a JSON **string** node.
///
/// `SAFE_CAST(… AS INT64)` on its own is wider than that — `BigQuery` reads a
/// hexadecimal literal, and trims surrounding whitespace — so extracting
/// through this pattern first is what makes the string case exact. Everything
/// it rejects, `json_get_int` also rejects, and returns NULL for.
const INT64_FROM_STR: &str = r"^[+-]?[0-9]+$";

/// The grammar Rust's `f64::FromStr` accepts, which is what `json_get_float`
/// applies to a JSON **string** node: an optional sign, then `inf`,
/// `infinity`, `nan` or a decimal with an optional exponent, case-insensitive.
/// A bare `.5` and a trailing `5.` are both accepted, and whitespace is not.
const FLOAT64_FROM_STR: &str =
    r"(?i)^[+-]?(inf(inity)?|nan|([0-9]+\.?[0-9]*|\.[0-9]+)(e[+-]?[0-9]+)?)$";

/// What `BigQuery` should be asked for, given a `json_get_*` call's path
/// arguments.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum JsonPath {
    /// A `BigQuery` JSON path — `$."a"."b"[0]` — reaching the same node the
    /// variadic path names.
    Path(String),
    /// The path can never resolve, so the call is NULL for every row.
    /// `datafusion-functions-json` maps a negative index and a NULL path
    /// element to its own `JsonPath::None`, which no lookup ever matches.
    NeverResolves,
}

/// Builds the `BigQuery` JSON path for a `json_get_*` call, or `None` when the
/// call has a shape `BigQuery` cannot be asked for.
///
/// The one shape with no translation is a **non-literal** path element:
/// `json_get_int(doc, key_col)` is legal — the functions are
/// `Signature::variadic_any` — and `BigQuery`'s JSON path argument must be a
/// constant, so there is nothing to emit. Such a call must not federate at all;
/// `crate::function_support` is what stops it, using this same function.
pub(crate) fn json_path(args: &[Expr]) -> Option<JsonPath> {
    // The first argument is the document; the rest are the path.
    let (_document, path) = args.split_first()?;
    if path.is_empty() {
        // `json_get_int(col)` reads the document itself. `$` is the JSONPath
        // for that, but what BigQuery returns for a bare `$` over a scalar
        // document is not pinned by anything here, so it is left to evaluate
        // locally rather than guessed at.
        return None;
    }

    let mut rendered = String::from("$");
    for element in path {
        let Expr::Literal(value, _) = element else {
            return None;
        };
        match value {
            ScalarValue::Utf8(Some(key))
            | ScalarValue::LargeUtf8(Some(key))
            | ScalarValue::Utf8View(Some(key)) => {
                // BigQuery's JSON_VALUE quotes a key with double quotes, and a
                // `"` inside one is backslash-escaped.
                let escaped = key.replace('\\', r"\\").replace('"', r#"\""#);
                let _ = write!(rendered, r#"."{escaped}""#);
            }
            ScalarValue::Int64(Some(index)) if *index >= 0 => {
                let _ = write!(rendered, "[{index}]");
            }
            ScalarValue::UInt64(Some(index)) => {
                let _ = write!(rendered, "[{index}]");
            }
            // A negative index, or a NULL path element of a type the function
            // accepts, resolves to nothing for every row.
            ScalarValue::Int64(Some(_) | None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Utf8View(None) => return Some(JsonPath::NeverResolves),
            // Any other literal type is rejected by the function's own
            // `return_type`, so it cannot reach a plan.
            _ => return None,
        }
    }
    Some(JsonPath::Path(rendered))
}

/// Whether the `BigQuery` dialect can translate this call, for the pushdown
/// policy to consult. Reads [`json_path`], so it answers exactly the question
/// the handlers below can answer.
#[must_use]
pub fn can_translate(call: &ScalarFunction) -> bool {
    match call.func.name() {
        JSON_GET_INT_NAME | JSON_GET_FLOAT_NAME => json_path(&call.args).is_some(),
        _ => true,
    }
}

/// `json_get_int(doc, path…)` →
/// `SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(doc, '<path>'), r'^[+-]?[0-9]+$') AS INT64)`.
///
/// `JSON_VALUE` renders the node at the path as a string and returns NULL for
/// an object, an array, a JSON `null` and a missing path — which is what
/// `json_get_int` returns for all four. `true` and `false` render as `"true"`
/// and `"false"`, which the pattern rejects, matching `json_get_int` again. A
/// number renders as its own token, so a float or an exponent form is rejected
/// exactly as `json_get_int` rejects it, and an integer outside `INT64` is a
/// NULL from `SAFE_CAST` exactly as it is a NULL from the `i64` conversion. The
/// pattern is what makes a JSON **string** node exact — see [`INT64_FROM_STR`].
pub(crate) fn json_get_int_to_sql(unparser: &Unparser, args: &[Expr]) -> Result<Option<ast::Expr>> {
    json_get_number_to_sql(
        unparser,
        args,
        JSON_GET_INT_NAME,
        INT64_FROM_STR,
        ast::DataType::Int64,
    )
}

/// `json_get_float(doc, path…)` → the same shape as
/// [`json_get_int_to_sql`], through [`FLOAT64_FROM_STR`] and `FLOAT64`.
pub(crate) fn json_get_float_to_sql(
    unparser: &Unparser,
    args: &[Expr],
) -> Result<Option<ast::Expr>> {
    json_get_number_to_sql(
        unparser,
        args,
        JSON_GET_FLOAT_NAME,
        FLOAT64_FROM_STR,
        ast::DataType::Float64,
    )
}

fn json_get_number_to_sql(
    unparser: &Unparser,
    args: &[Expr],
    function: &str,
    pattern: &str,
    cast_to: ast::DataType,
) -> Result<Option<ast::Expr>> {
    let (Some(document), Some(path)) = (args.first(), json_path(args)) else {
        // Unreachable with the deny-list installed, which refuses exactly the
        // calls `json_path` cannot render. Reachable only if this dialect is
        // used without `deny_spice_functions_for_bigquery_table_providers`, and
        // there the alternative — returning `Ok(None)` — makes the unparser
        // emit `json_get_*` verbatim into BigQuery SQL, which is the wrong
        // answer dressed as a remote error. Fail where it can be read instead.
        return Err(DataFusionError::Plan(format!(
            "Cannot push down '{function}' to BigQuery: its path arguments must all be literals. \
             This plan should not have federated; the BigQuery function-support policy is missing. \
             See: https://spiceai.org/docs/components/data-connectors/adbc"
        )));
    };

    let path = match path {
        JsonPath::NeverResolves => return Ok(Some(cast_null_to(cast_to))),
        JsonPath::Path(path) => path,
    };

    let document = unparser.expr_to_sql(document)?;

    let json_value = call_function(
        "JSON_VALUE",
        vec![document, ast::Expr::Value(single_quoted(&path).into())],
    );
    let matched = call_function(
        "REGEXP_EXTRACT",
        vec![json_value, ast::Expr::Value(raw_string(pattern).into())],
    );

    Ok(Some(ast::Expr::Cast {
        kind: ast::CastKind::SafeCast,
        expr: Box::new(matched),
        data_type: cast_to,
        array: false,
        format: None,
    }))
}

fn cast_null_to(data_type: ast::DataType) -> ast::Expr {
    ast::Expr::Cast {
        kind: ast::CastKind::Cast,
        expr: Box::new(ast::Expr::Value(ast::Value::Null.into())),
        data_type,
        array: false,
        format: None,
    }
}

fn call_function(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Function(Function {
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(name))]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        parameters: ast::FunctionArguments::None,
        uses_odbc_syntax: false,
    })
}

fn single_quoted(value: &str) -> ast::Value {
    ast::Value::SingleQuotedString(value.to_string())
}

/// A `BigQuery` raw string literal, `r'…'`. Raw so a backslash in the pattern
/// is a regex escape rather than a string escape.
fn raw_string(value: &str) -> ast::Value {
    ast::Value::SingleQuotedRawStringLiteral(value.to_string())
}

/// [`BigQueryDialect`] plus Spice's own scalar-function handlers.
///
/// `BigQueryDialect` does not implement [`Dialect::with_custom_scalar_overrides`]
/// — it panics — so the handlers cannot be attached to it directly. This wraps
/// it instead, holding the handler map itself and forwarding every other
/// [`Dialect`] method to the inner dialect. **Every** method is forwarded
/// explicitly: inheriting a trait default here would silently unparse
/// `BigQuery` SQL as if it were the generic dialect, changing quoting, casts
/// and interval rendering with no error anywhere.
pub struct SpiceBigQueryDialect {
    inner: BigQueryDialect,
    custom_scalar_fn_overrides: HashMap<String, ScalarFnToSqlHandler>,
}

impl Default for SpiceBigQueryDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SpiceBigQueryDialect {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: BigQueryDialect::new(),
            custom_scalar_fn_overrides: HashMap::new(),
        }
    }
}

impl Dialect for SpiceBigQueryDialect {
    fn with_custom_scalar_overrides(mut self, handlers: Vec<(&str, ScalarFnToSqlHandler)>) -> Self {
        for (name, handler) in handlers {
            self.custom_scalar_fn_overrides
                .insert(name.to_string(), handler);
        }
        self
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if let Some(handler) = self.custom_scalar_fn_overrides.get(func_name) {
            return handler(unparser, args);
        }
        self.inner
            .scalar_function_to_sql_overrides(unparser, func_name, args)
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }

    fn use_array_keyword_for_array_literals(&self) -> bool {
        self.inner.use_array_keyword_for_array_literals()
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        self.inner.supports_nulls_first_in_sort()
    }

    fn use_timestamp_for_date64(&self) -> bool {
        self.inner.use_timestamp_for_date64()
    }

    fn interval_style(&self) -> IntervalStyle {
        self.inner.interval_style()
    }

    fn float64_ast_dtype(&self) -> ast::DataType {
        self.inner.float64_ast_dtype()
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.utf8_cast_dtype()
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.large_utf8_cast_dtype()
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        self.inner.date_field_extract_style()
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        self.inner.character_length_style()
    }

    fn int64_cast_dtype(&self) -> ast::DataType {
        self.inner.int64_cast_dtype()
    }

    fn int8_cast_dtype(&self) -> ast::DataType {
        self.inner.int8_cast_dtype()
    }

    fn int32_cast_dtype(&self) -> ast::DataType {
        self.inner.int32_cast_dtype()
    }

    fn timestamp_cast_dtype(&self, time_unit: &TimeUnit, tz: &Option<Arc<str>>) -> ast::DataType {
        self.inner.timestamp_cast_dtype(time_unit, tz)
    }

    fn timestamp_at_time_zone_to_sql(&self, input: ast::Expr, tz: &str) -> Option<ast::Expr> {
        self.inner.timestamp_at_time_zone_to_sql(input, tz)
    }

    fn date32_cast_dtype(&self) -> ast::DataType {
        self.inner.date32_cast_dtype()
    }

    fn supports_column_alias_in_table_alias(&self) -> bool {
        self.inner.supports_column_alias_in_table_alias()
    }

    fn requires_derived_table_alias(&self) -> bool {
        self.inner.requires_derived_table_alias()
    }

    fn division_operator(&self) -> BinaryOperator {
        self.inner.division_operator()
    }

    fn higher_order_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        self.inner
            .higher_order_function_to_sql_overrides(unparser, func_name, args)
    }

    fn window_func_support_window_frame(
        &self,
        func_name: &str,
        start_bound: &WindowFrameBound,
        end_bound: &WindowFrameBound,
    ) -> bool {
        self.inner
            .window_func_support_window_frame(func_name, start_bound, end_bound)
    }

    fn full_qualified_col(&self) -> bool {
        self.inner.full_qualified_col()
    }

    fn unnest_as_table_factor(&self) -> bool {
        self.inner.unnest_as_table_factor()
    }

    fn unnest_as_lateral_flatten(&self) -> bool {
        self.inner.unnest_as_lateral_flatten()
    }

    fn col_alias_overrides(&self, alias: &str) -> Result<Option<String>> {
        self.inner.col_alias_overrides(alias)
    }

    fn supports_qualify(&self) -> bool {
        self.inner.supports_qualify()
    }

    fn timestamp_with_tz_to_string(&self, dt: DateTime<Tz>, unit: TimeUnit) -> String {
        self.inner.timestamp_with_tz_to_string(dt, unit)
    }

    fn supports_empty_select_list(&self) -> bool {
        self.inner.supports_empty_select_list()
    }

    fn string_literal_to_sql(&self, s: &str) -> Option<ast::Expr> {
        self.inner.string_literal_to_sql(s)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
    use datafusion::prelude::{col, lit};
    use datafusion::sql::unparser::Unparser;

    use super::{
        JSON_GET_FLOAT_NAME, JSON_GET_INT_NAME, JsonPath, SpiceBigQueryDialect, can_translate,
        json_path,
    };
    use crate::dialect::new_bigquery_dialect;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::expr::ScalarFunction;

    fn json_udf(name: &str, arity: usize) -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![DataType::Utf8; arity],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ))
    }

    fn call(name: &str, args: Vec<Expr>) -> ScalarFunction {
        ScalarFunction::new_udf(json_udf(name, args.len()), args)
    }

    /// The SQL the `BigQuery` dialect renders for one `json_get_*` call.
    fn render(name: &str, args: Vec<Expr>) -> String {
        let dialect = new_bigquery_dialect();
        Unparser::new(dialect.as_ref())
            .expr_to_sql(&Expr::ScalarFunction(call(name, args)))
            .expect("the BigQuery dialect renders this call")
            .to_string()
    }

    #[test]
    fn a_literal_key_becomes_a_bigquery_json_path() {
        assert_eq!(
            json_path(&[col("doc"), lit("a")]),
            Some(JsonPath::Path(r#"$."a""#.to_string()))
        );
    }

    #[test]
    fn the_variadic_path_becomes_one_json_path() {
        assert_eq!(
            json_path(&[col("doc"), lit("a"), lit("b"), lit(0_i64)]),
            Some(JsonPath::Path(r#"$."a"."b"[0]"#.to_string()))
        );
    }

    #[test]
    fn a_dotted_key_stays_one_key() {
        // `json_get_int(doc, 'a.b')` reads a key literally named `a.b`. Quoting
        // it is what stops BigQuery reading it as two steps, which would return
        // a different value rather than an error.
        assert_eq!(
            json_path(&[col("doc"), lit("a.b")]),
            Some(JsonPath::Path(r#"$."a.b""#.to_string()))
        );
    }

    #[test]
    fn a_key_containing_a_quote_or_a_backslash_is_escaped() {
        assert_eq!(
            json_path(&[col("doc"), lit(r#"a"b"#)]),
            Some(JsonPath::Path(r#"$."a\"b""#.to_string()))
        );
        assert_eq!(
            json_path(&[col("doc"), lit(r"a\b")]),
            Some(JsonPath::Path(r#"$."a\\b""#.to_string()))
        );
    }

    #[test]
    fn a_path_that_can_never_resolve_is_recognised_as_such() {
        // A negative index and a NULL path element both map to
        // `datafusion-functions-json`'s own `JsonPath::None`, which matches
        // nothing, so the call is NULL for every row.
        for element in [
            lit(-1_i64),
            Expr::Literal(ScalarValue::Utf8(None), None),
            Expr::Literal(ScalarValue::Int64(None), None),
        ] {
            assert_eq!(
                json_path(&[col("doc"), element.clone()]),
                Some(JsonPath::NeverResolves),
                "{element} names nothing"
            );
        }
    }

    #[test]
    fn a_non_literal_path_element_has_no_translation() {
        // BigQuery's JSON path argument must be a constant, so there is nothing
        // to render for a per-row path.
        assert_eq!(json_path(&[col("doc"), col("key")]), None);
        assert_eq!(json_path(&[col("doc"), lit("a"), col("key")]), None);
    }

    #[test]
    fn a_call_with_no_path_has_no_translation() {
        assert_eq!(json_path(&[col("doc")]), None);
    }

    #[test]
    fn can_translate_answers_for_the_json_functions_and_defers_on_the_rest() {
        assert!(can_translate(&call(
            JSON_GET_INT_NAME,
            vec![col("doc"), lit("a")]
        )));
        assert!(can_translate(&call(
            JSON_GET_FLOAT_NAME,
            vec![col("doc"), lit("a")]
        )));
        assert!(!can_translate(&call(
            JSON_GET_INT_NAME,
            vec![col("doc"), col("key")]
        )));
        assert!(!can_translate(&call(
            JSON_GET_FLOAT_NAME,
            vec![col("doc"), col("key")]
        )));
        assert!(
            can_translate(&call("upper", vec![col("doc"), col("key")])),
            "a function this dialect has no handler for is not this check's business"
        );
    }

    #[test]
    fn json_get_int_renders_as_a_guarded_safe_cast() {
        assert_eq!(
            render(JSON_GET_INT_NAME, vec![col("doc"), lit("a")]),
            r#"SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(`doc`, '$."a"'), R'^[+-]?[0-9]+$') AS INT64)"#
        );
    }

    #[test]
    fn json_get_float_renders_as_a_guarded_safe_cast() {
        assert_eq!(
            render(JSON_GET_FLOAT_NAME, vec![col("doc"), lit("a"), lit(2_i64)]),
            r#"SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(`doc`, '$."a"[2]'), R'(?i)^[+-]?(inf(inity)?|nan|([0-9]+\.?[0-9]*|\.[0-9]+)(e[+-]?[0-9]+)?)$') AS FLOAT64)"#
        );
    }

    #[test]
    fn a_path_that_can_never_resolve_renders_as_a_typed_null() {
        assert_eq!(
            render(JSON_GET_INT_NAME, vec![col("doc"), lit(-1_i64)]),
            "CAST(NULL AS INT64)"
        );
        assert_eq!(
            render(JSON_GET_FLOAT_NAME, vec![col("doc"), lit(-1_i64)]),
            "CAST(NULL AS FLOAT64)"
        );
    }

    #[test]
    fn no_rendering_ever_contains_the_function_verbatim() {
        for name in [JSON_GET_INT_NAME, JSON_GET_FLOAT_NAME] {
            for args in [
                vec![col("doc"), lit("a")],
                vec![col("doc"), lit("a"), lit(0_i64)],
                vec![col("doc"), lit(-1_i64)],
            ] {
                let sql = render(name, args);
                assert!(
                    !sql.contains(name),
                    "{name} must not reach BigQuery SQL: {sql}"
                );
            }
        }
    }

    #[test]
    fn an_untranslatable_call_fails_rather_than_unparsing_verbatim() {
        // Unreachable with the deny-list installed. If the dialect is used
        // without it, the alternative is emitting `json_get_int(...)` into
        // BigQuery SQL, so this fails where a reader can see it instead.
        let dialect = new_bigquery_dialect();
        let error = Unparser::new(dialect.as_ref())
            .expr_to_sql(&Expr::ScalarFunction(call(
                JSON_GET_INT_NAME,
                vec![col("doc"), col("key")],
            )))
            .expect_err("a dynamic path has no BigQuery translation");
        assert!(
            error.to_string().contains("must all be literals"),
            "the error must say what about the call cannot be pushed down: {error}"
        );
    }

    #[test]
    fn the_dialect_renders_every_name_the_deny_list_carves_out() {
        // The carve-out is what lets these names federate. A name in it that
        // the dialect has no handler for would be unparsed verbatim, so the
        // list is only safe while the dialect answers for all of it.
        let dialect = new_bigquery_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        for name in crate::dialect::bigquery_native_function_names() {
            let rendered = dialect
                .scalar_function_to_sql_overrides(&unparser, name, &[col("doc"), lit("a")])
                .unwrap_or_else(|error| panic!("the dialect must render `{name}`: {error}"));
            assert!(
                rendered.is_some(),
                "`{name}` is carved out of the deny-list but the dialect has no handler for it, \
                 so it would be unparsed verbatim into BigQuery SQL"
            );
        }
    }

    #[test]
    fn a_predicate_over_a_json_function_unparses_into_the_statement() {
        // The whole statement, not just the expression: this is the SQL a
        // federated scan sends, so it is where a verbatim `json_get_*` would
        // show up.
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("doc", DataType::Utf8, true),
        ]));
        let source = Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
            schema,
        )) as Arc<dyn datafusion::logical_expr::TableSource>;
        let plan = datafusion::logical_expr::LogicalPlanBuilder::scan("t", source, None)
            .expect("scan t")
            .filter(
                Expr::ScalarFunction(call(JSON_GET_INT_NAME, vec![col("doc"), lit("a")]))
                    .eq(lit(1_i64)),
            )
            .expect("filter")
            .build()
            .expect("build");

        let dialect = new_bigquery_dialect();
        let sql = Unparser::new(dialect.as_ref())
            .plan_to_sql(&plan)
            .expect("the BigQuery dialect unparses the plan")
            .to_string();

        assert!(sql.contains("JSON_VALUE("), "no JSON_VALUE in: {sql}");
        assert!(sql.contains("SAFE_CAST("), "no SAFE_CAST in: {sql}");
        assert!(
            !sql.contains("json_get_"),
            "a Spice-only function reached BigQuery SQL: {sql}"
        );
    }

    #[test]
    fn the_wrapper_unparses_exactly_as_the_bigquery_dialect_does() {
        // Every `Dialect` method is forwarded to the inner dialect. Inheriting
        // a trait default for one instead would change quoting, a cast's type
        // name or an alias silently — valid SQL that BigQuery rejects, or worse,
        // accepts differently. Unparsing the same plan through both is what
        // notices, whichever method was missed.
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("n", DataType::Int64, false),
            datafusion::arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                    Some("UTC".into()),
                ),
                true,
            ),
        ]));
        let source = Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
            schema,
        )) as Arc<dyn datafusion::logical_expr::TableSource>;
        let plan = datafusion::logical_expr::LogicalPlanBuilder::scan("t", source, None)
            .expect("scan t")
            .project(vec![
                datafusion::prelude::cast(col("n"), DataType::Float64).alias("a.b"),
                datafusion::prelude::cast(col("n"), DataType::Utf8),
                datafusion::prelude::cast(col("ts"), DataType::Date32),
                col("ts").alias("raw"),
            ])
            .expect("project")
            .build()
            .expect("build");

        let unparse = |dialect: &dyn datafusion::sql::unparser::dialect::Dialect| {
            Unparser::new(dialect)
                .plan_to_sql(&plan)
                .expect("unparse the plan")
                .to_string()
        };

        assert_eq!(
            unparse(&SpiceBigQueryDialect::new()),
            unparse(&datafusion::sql::unparser::dialect::BigQueryDialect::new()),
            "a `Dialect` method is not forwarded to the inner BigQuery dialect"
        );
    }
}
