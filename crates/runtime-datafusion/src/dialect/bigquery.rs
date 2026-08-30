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

//! The `BigQuery` translation of `json_get_int`, and the `BigQuery` dialect
//! that installs it. [`SCALAR_OVERRIDES`] is the whole of what this dialect
//! rewrites, and says what a further entry costs.
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

/// The grammar Rust's `i64::FromStr` accepts, which is what
/// `json_get_int` applies to a JSON **string** node.
///
/// `SAFE_CAST(… AS INT64)` on its own is wider than that — `BigQuery` reads a
/// hexadecimal literal, and trims surrounding whitespace — so extracting
/// through this pattern first is what makes the string case exact. Everything
/// it rejects, `json_get_int` also rejects, and returns NULL for.
///
/// It also agrees at the boundaries: an integer too large for `i64` is NULL on
/// both sides, because Rust's `i64::FromStr` fails rather than saturating.
///
/// Any group must be non-capturing. `REGEXP_EXTRACT` accepts **at most one**
/// capturing group and errors on more, which would fail every federated call
/// remotely; with none it returns the whole match, which is what this wants.
/// [`tests::no_pattern_has_a_capturing_group`] holds the pattern to that.
const INT64_FROM_STR: &str = r"^[+-]?[0-9]+$";

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
                if !key_is_renderable(key) {
                    return None;
                }
                // BigQuery quotes a key with double quotes, which is what lets
                // a key containing `.` stay one key.
                let _ = write!(rendered, r#"."{key}""#);
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

/// Whether a key can be written into the path at all.
///
/// The path is emitted as a `BigQuery` **raw** string literal, so what is
/// written is what the JSON path parser reads: nothing processes backslashes in
/// between. That holds only while the key contains nothing either layer would
/// read as structure — `\'` would end the literal, `"` would end the quoted
/// field name, `\\` is an escape to the JSON path parser, and a control
/// character has no agreed spelling in either. Escaping across two layers with
/// different rules is how a key silently becomes a *different* path, so such a
/// key is left for the local engine instead.
fn key_is_renderable(key: &str) -> bool {
    !key.contains(['\'', '"', '\\']) && !key.chars().any(char::is_control)
}

/// Whether the path arguments of a `json_get_*` call can be rendered. Reads
/// [`json_path`], so it answers exactly the question the handlers can answer.
fn json_path_is_renderable(args: &[Expr]) -> bool {
    json_path(args).is_some()
}

/// Whether the `BigQuery` dialect can translate this call, for the pushdown
/// policy to consult.
///
/// A function with no entry in [`SCALAR_OVERRIDES`] is not this check's
/// business: the deny-list has not carved it out, so it is already denied.
#[must_use]
pub fn can_translate(call: &ScalarFunction) -> bool {
    let name = call.func.name();
    SCALAR_OVERRIDES
        .iter()
        .find(|entry| entry.name == name)
        .is_none_or(|entry| (entry.can_translate)(&call.args))
}

/// A function the `BigQuery` dialect rewrites into native SQL.
///
/// The handler and the per-call check are one entry because they are one fact:
/// a handler that can only render some call shapes is safe only while the
/// deny-list refuses the rest. Splitting them into a list and a separate match
/// is what lets a later partial handler be carved out of the deny-list with
/// nothing to refuse its untranslatable shapes, which puts the function name
/// verbatim into the remote SQL.
pub(crate) struct ScalarOverride {
    pub(crate) name: &'static str,
    /// Renders the call, or fails if it cannot — see [`json_call_to_sql`].
    pub(crate) handler: fn(&Unparser, &[Expr]) -> Result<Option<ast::Expr>>,
    /// Whether `handler` can render a call with these arguments.
    pub(crate) can_translate: fn(&[Expr]) -> bool,
}

/// Every function the `BigQuery` dialect rewrites, with what each consumer
/// needs. [`crate::dialect`] derives the dialect's handlers, the deny-list
/// carve-out, and the per-call check from this one table.
///
/// An entry here is a claim that `BigQuery` answers what the local function
/// answers, for **every** document and path the call shapes in `can_translate`
/// admit — and it is enforced as a wrong row rather than an error, since the
/// unparsed SQL is whatever this says it is. So the bar for an entry is a
/// mapping measured against a real `BigQuery`, not one derived from what its
/// documentation implies: the disagreements that matter live at the boundaries
/// (an out-of-range magnitude, a duplicate object member, member order, a node
/// of the wrong JSON type), which is exactly where documented behaviour is
/// thinnest. `crate::function_support::tests::bigquery_pushes_down_exactly_one_json_function`
/// asserts the list whole, so an entry cannot be added quietly.
pub(crate) const SCALAR_OVERRIDES: &[ScalarOverride] = &[ScalarOverride {
    name: JSON_GET_INT_NAME,
    handler: json_get_int_to_sql,
    can_translate: json_path_is_renderable,
}];

/// Renders one `json_get_*` call: pulls out the document and the JSON path,
/// and hands both to `render` as `BigQuery` SQL.
///
/// Every handler goes through here so the failure below is written once, and
/// takes the function's name so the message can say which call failed. It is
/// unreachable with the deny-list installed, which refuses exactly the calls
/// [`json_path`] cannot render. Reachable only if this dialect is used without
/// `deny_spice_functions_for_bigquery_table_providers`, and there the
/// alternative — returning `Ok(None)` — makes the unparser emit `json_get_*`
/// verbatim into `BigQuery` SQL, which is the wrong answer dressed as a remote
/// error. Fail where it can be read instead.
///
/// `null_type` is what a path that can never resolve renders as. It carries the
/// function's own return type: an untyped NULL would leave the federated schema
/// disagreeing with the plan's.
fn json_call_to_sql(
    unparser: &Unparser,
    args: &[Expr],
    function: &str,
    null_type: ast::DataType,
    render: impl FnOnce(ast::Expr, ast::Expr) -> ast::Expr,
) -> Result<Option<ast::Expr>> {
    let (Some(document), Some(path)) = (args.first(), json_path(args)) else {
        return Err(DataFusionError::Plan(format!(
            "Failed to run this query against BigQuery: '{function}' was called in a form BigQuery \
             cannot express, so the query cannot be completed. BigQuery needs a constant JSON path \
             built only from plain keys: every path argument must be a literal, there must be at \
             least one, and a key cannot contain a quote, a backslash or a control character. \
             Rewrite the call to a constant path of plain keys, or set 'query_federation: disabled' \
             on the dataset to evaluate it locally instead. \
             See: https://spiceai.org/docs/components/data-connectors/adbc"
        )));
    };

    let path = match path {
        JsonPath::NeverResolves => return Ok(Some(cast_null_to(null_type))),
        JsonPath::Path(path) => path,
    };

    Ok(Some(render(
        unparser.expr_to_sql(document)?,
        ast::Expr::Value(raw_string(&path).into()),
    )))
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
    json_call_to_sql(
        unparser,
        args,
        JSON_GET_INT_NAME,
        ast::DataType::Int64,
        |document, path| ast::Expr::Cast {
            kind: ast::CastKind::SafeCast,
            expr: Box::new(call_function(
                "REGEXP_EXTRACT",
                vec![
                    call_function("JSON_VALUE", vec![document, path]),
                    ast::Expr::Value(raw_string(INT64_FROM_STR).into()),
                ],
            )),
            data_type: ast::DataType::Int64,
            array: false,
            format: None,
        },
    )
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

/// A `BigQuery` raw string literal, `r'…'`. Raw so a backslash in the value is
/// the value's own, not something the SQL string layer consumes first — which
/// matters for a regex escape and for a JSON path alike.
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
        INT64_FROM_STR, JSON_GET_INT_NAME, JsonPath, SpiceBigQueryDialect, can_translate, json_path,
    };
    use crate::dialect::{bigquery_native_function_names, new_bigquery_dialect};

    /// The JSON functions this dialect deliberately leaves to the local engine.
    /// Every one of them is a name `datafusion-functions-json` registers, so a
    /// carve-out for it would federate — which is what these hold shut.
    const UNTRANSLATED: &[&str] = &[
        "json_get_str",
        "json_get_bool",
        "json_get_float",
        "json_length",
        "json_len",
        "json_object_keys",
        "json_keys",
    ];
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
    fn a_key_the_two_quoting_layers_disagree_about_is_not_translated() {
        // A quote, a backslash or a control character means one of the SQL
        // literal layer and the JSONPath layer would read the key as structure.
        // Escaping across both is how a key silently becomes a different path,
        // so these are left for the local engine.
        for key in [r#"a"b"#, r"a\b", "a'b", "a\nb", "a\tb"] {
            assert_eq!(
                json_path(&[col("doc"), lit(key)]),
                None,
                "{key:?} cannot be written into a BigQuery JSON path unambiguously"
            );
        }
    }

    #[test]
    fn no_pattern_has_a_capturing_group() {
        // `REGEXP_EXTRACT` accepts at most one capturing group and errors on
        // more; with none it returns the whole match. A capturing group added
        // here would fail every federated call at BigQuery, which no local test
        // can see.
        for pattern in [INT64_FROM_STR] {
            let mut chars = pattern.chars().peekable();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => {
                        chars.next();
                    }
                    '(' => assert_eq!(
                        chars.peek(),
                        Some(&'?'),
                        "capturing group in {pattern}: every group must be `(?:…)`"
                    ),
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn the_integer_grammar_accepts_what_rust_accepts_and_nothing_more() {
        // Every case is asserted against Rust's own parser as well as the
        // pattern, so the two cannot drift apart while the test still passes.
        // `regex` and BigQuery's engine are both RE2 lineage, so a pattern this
        // one accepts is one BigQuery accepts.
        let pattern = regex::Regex::new(INT64_FROM_STR).expect("the integer grammar compiles");
        for accepted in [
            "0",
            "1",
            "-1",
            "+1",
            "9223372036854775807",
            "-9223372036854775808",
        ] {
            assert!(
                pattern.is_match(accepted),
                "`{accepted}` parses as i64 in Rust, so the pattern must accept it"
            );
            assert!(
                accepted.parse::<i64>().is_ok(),
                "`{accepted}` must actually parse in Rust, or this test is asserting the wrong thing"
            );
        }
        // `0x2A` and `  1  ` are the pair SAFE_CAST reads and Rust does not,
        // which is the whole reason the extraction happens before the cast.
        for rejected in ["0x2A", "  1  ", "1.0", "1e3", "", "-", "1,5", "true"] {
            assert!(
                !pattern.is_match(rejected),
                "`{rejected}` is not an i64 in Rust, so the pattern must reject it"
            );
            assert!(
                rejected.parse::<i64>().is_err(),
                "`{rejected}` must actually fail in Rust, or this test is asserting the wrong thing"
            );
        }
    }

    /// An integer outside `i64` is the boundary the two sides agree on for the
    /// opposite reason to the float form: the pattern matches the digits, and
    /// both Rust's `i64::FromStr` and `SAFE_CAST(… AS INT64)` decline the value
    /// rather than saturating, so both are NULL.
    #[test]
    fn the_integer_grammar_leaves_an_out_of_range_magnitude_to_the_cast() {
        let pattern = regex::Regex::new(INT64_FROM_STR).expect("the integer grammar compiles");
        for out_of_range in ["9223372036854775808", "-9223372036854775809"] {
            assert!(
                pattern.is_match(out_of_range),
                "`{out_of_range}` is a run of digits, so the pattern matches it"
            );
            assert!(
                out_of_range.parse::<i64>().is_err(),
                "`{out_of_range}` must not parse, or the NULL the cast gives has no counterpart"
            );
        }
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
        assert!(!can_translate(&call(
            JSON_GET_INT_NAME,
            vec![col("doc"), col("key")]
        )));
        for name in UNTRANSLATED.iter().chain(&["json_contains", "upper"]) {
            assert!(
                can_translate(&call(name, vec![col("doc"), col("key")])),
                "{name} has no handler in this dialect, so it is not this check's business — \
                 the deny-list has not carved it out and it cannot federate at all"
            );
        }
    }

    #[test]
    fn json_get_int_renders_as_a_guarded_safe_cast() {
        assert_eq!(
            render(JSON_GET_INT_NAME, vec![col("doc"), lit("a")]),
            r#"SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(`doc`, R'$."a"'), R'^[+-]?[0-9]+$') AS INT64)"#
        );
    }

    #[test]
    fn an_index_in_the_path_renders_after_the_key() {
        assert_eq!(
            render(JSON_GET_INT_NAME, vec![col("doc"), lit("a"), lit(2_i64)]),
            r#"SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(`doc`, R'$."a"[2]'), R'^[+-]?[0-9]+$') AS INT64)"#
        );
    }

    #[test]
    fn a_path_that_can_never_resolve_renders_as_a_typed_null() {
        assert_eq!(
            render(JSON_GET_INT_NAME, vec![col("doc"), lit(-1_i64)]),
            "CAST(NULL AS INT64)"
        );
    }

    #[test]
    fn no_rendering_ever_contains_the_function_verbatim() {
        for name in [JSON_GET_INT_NAME] {
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
        let message = error.to_string();
        for expected in [
            // The call shape that failed, not the policy that should have
            // caught it: an internal cause is no help to whoever ran the query.
            "must be a literal",
            // The way out, and where to read about it.
            "query_federation",
            "https://spiceai.org/docs/components/data-connectors/adbc",
        ] {
            assert!(
                message.contains(expected),
                "the error must carry {expected:?}: {message}"
            );
        }
        assert!(
            !message.contains("policy"),
            "the error must not name an internal mechanism: {message}"
        );
    }

    #[test]
    fn the_dialect_renders_every_name_the_deny_list_carves_out() {
        // The carve-out is what lets these names federate. A name in it that
        // the dialect has no handler for would be unparsed verbatim, so the
        // list is only safe while the dialect answers for all of it.
        let dialect = new_bigquery_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        for name in bigquery_native_function_names() {
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
    fn the_carve_out_is_one_name_wide() {
        assert_eq!(
            bigquery_native_function_names(),
            vec![JSON_GET_INT_NAME],
            "`json_get_int` is the only JSON function whose BigQuery rendering has been \
             measured to answer what the local function answers"
        );
    }

    #[test]
    fn the_untranslated_json_families_have_no_rendering_at_all() {
        // No handler is what makes the deny-list the single gate for these: the
        // dialect has nothing to offer the unparser, so the only thing standing
        // between them and a `json_get_float(...)` verbatim in BigQuery SQL is
        // that they never federate. `crate::function_support` is where that is
        // asserted; this is the half that says the dialect cannot quietly grow
        // a rendering the carve-out has not accounted for.
        let dialect = new_bigquery_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        for name in UNTRANSLATED {
            let rendered = dialect
                .scalar_function_to_sql_overrides(&unparser, name, &[col("doc"), lit("a")])
                .unwrap_or_else(|error| panic!("the dialect must not fail on `{name}`: {error}"));
            assert!(
                rendered.is_none(),
                "`{name}` has a BigQuery rendering but is not carved out of the deny-list; one \
                 of the two is wrong"
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
